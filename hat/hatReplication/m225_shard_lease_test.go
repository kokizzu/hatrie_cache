package hatReplication

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestShardLeaseRegistryRejectsDuplicateOwnersAndFencesStaleLeases(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}

	first, err := registry.Acquire("region-a", "node-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.FencingToken != 1 || !first.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected first lease: %+v", first)
	}
	if _, err := registry.Acquire("region-a", "node-a", time.Minute, now); !errors.Is(err, ErrShardLeaseHeld) {
		t.Fatalf("same owner should not acquire twice, got %v", err)
	}
	if _, err := registry.Acquire("region-a", "node-b", time.Minute, now); !errors.Is(err, ErrShardLeaseHeld) {
		t.Fatalf("duplicate owner should be rejected, got %v", err)
	}

	renewed, err := registry.Renew(first, 2*time.Minute, now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.ExpiresAt.Equal(now.Add(2*time.Minute + 10*time.Second)) {
		t.Fatalf("unexpected renewed lease: %+v", renewed)
	}

	second, err := registry.Acquire("region-a", "node-b", time.Minute, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if second.FencingToken != 2 {
		t.Fatalf("expected fencing token 2 after expiry takeover, got %d", second.FencingToken)
	}
	if err := registry.Validate(first, now.Add(3*time.Minute)); !errors.Is(err, ErrShardLeaseFenced) {
		t.Fatalf("stale lease should be fenced by validation, got %v", err)
	}
	if err := registry.Validate(second, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Renew(first, time.Minute, now.Add(3*time.Minute)); !errors.Is(err, ErrShardLeaseFenced) {
		t.Fatalf("stale lease should be fenced on renew, got %v", err)
	}
	if err := registry.Release(first, now.Add(3*time.Minute)); !errors.Is(err, ErrShardLeaseFenced) {
		t.Fatalf("stale lease should be fenced on release, got %v", err)
	}
	if err := registry.Release(second, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Get("region-a", now.Add(3*time.Minute)); ok {
		t.Fatal("released lease should not remain visible")
	}
}

func TestShardLeaseRegistrySnapshotRoundTripRetainsFencingHistory(t *testing.T) {
	now := time.Unix(1_700_000_000, 123).UTC()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 4})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire("region-b", "node-a", time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Release(lease, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	active, err := registry.Acquire("region-a", "node-b", 2*time.Hour, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want := registry.Snapshot()
	encoded, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded ShardLeaseRegistrySnapshot
	if err := decoded.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("snapshot changed across binary round trip:\nwant=%+v\n got=%+v", want, decoded)
	}

	restored, err := NewShardLeaseRegistryFromSnapshot(ShardLeaseRegistryOptions{MaxLeases: 4}, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored registry differs:\nwant=%+v\n got=%+v", want, got)
	}
	if err := restored.Release(active, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	next, err := restored.Acquire("region-a", "node-c", time.Minute, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if next.FencingToken != active.FencingToken+1 {
		t.Fatalf("restore reused a fencing token: active=%d next=%d", active.FencingToken, next.FencingToken)
	}
}

func TestShardLeaseRegistryRejectsMalformedSnapshots(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire("region-a", "node-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := registry.Snapshot().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("invalid"),
	} {
		var snapshot ShardLeaseRegistrySnapshot
		if err := snapshot.UnmarshalBinary(malformed); !errors.Is(err, ErrShardLeaseSnapshotInvalid) {
			t.Errorf("expected malformed snapshot error, got %v", err)
		}
	}
	if lease.FencingToken != 1 {
		t.Fatal("malformed snapshot test corrupted source lease")
	}
	if bytes.Equal(encoded, nil) {
		t.Fatal("snapshot must not be empty")
	}
}

func TestShardLeaseRegistryRestoreIsAtomicOnInvalidSnapshot(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire("region-a", "node-a", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	invalid := ShardLeaseRegistrySnapshot{
		LastFencingToken: lease.FencingToken,
		Leases: []ShardLease{
			lease,
			lease,
		},
	}
	if err := registry.Restore(invalid); !errors.Is(err, ErrShardLeaseSnapshotInvalid) {
		t.Fatalf("expected invalid snapshot error, got %v", err)
	}
	if err := registry.Validate(lease, now.Add(time.Second)); err != nil {
		t.Fatalf("invalid restore changed the live registry: %v", err)
	}
}

func TestShardLeaseRegistryConcurrentAcquireHasSingleWinner(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 32
	var wg sync.WaitGroup
	wg.Add(contenders)
	winners := make(chan ShardLease, contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wg.Done()
			lease, err := registry.Acquire("region-a", "node-"+string(rune('a'+index)), time.Minute, now)
			if err == nil {
				winners <- lease
			} else if !errors.Is(err, ErrShardLeaseHeld) {
				t.Errorf("unexpected acquire error: %v", err)
			}
		}(index)
	}
	wg.Wait()
	close(winners)
	count := 0
	for range winners {
		count++
	}
	if count != 1 {
		t.Fatalf("expected one lease winner, got %d", count)
	}
}
