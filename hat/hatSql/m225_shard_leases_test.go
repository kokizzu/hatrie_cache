package hatSql_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

type m225Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *m225Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *m225Clock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func m225Registry(t *testing.T, clock *m225Clock) *hatSql.SQLShardLeaseRegistry {
	t.Helper()
	registry, err := hatSql.NewSQLShardLeaseRegistry(hatSql.SQLShardLeaseRegistryOptions{
		ShardCount:      4,
		MaxShards:       16,
		LeaseDuration:   10 * time.Second,
		MaxOwnerBytes:   64,
		MaxShardIDBytes: 64,
		Now:             clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestM225ShardLeaseFencesDuplicateOwnershipAndRestores(t *testing.T) {
	clock := &m225Clock{now: time.Unix(1_700_000_000, 0).UTC()}
	registry := m225Registry(t, clock)

	first, err := registry.Acquire("region-a/0", "worker-a")
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	if first.FencingToken != 1 {
		t.Fatalf("first fencing token = %d, want 1", first.FencingToken)
	}
	if got, want := first.ExpiresAt, clock.Now().Add(10*time.Second); !got.Equal(want) {
		t.Fatalf("first expiry = %s, want %s", got, want)
	}

	if _, err := registry.Acquire("region-a/0", "worker-b"); !errors.Is(err, hatSql.ErrSQLShardLeaseHeld) {
		t.Fatalf("duplicate Acquire error = %v, want ErrSQLShardLeaseHeld", err)
	}
	renewed, err := registry.Renew(first)
	if err != nil {
		t.Fatalf("Renew(first) error = %v", err)
	}
	if renewed.FencingToken != first.FencingToken {
		t.Fatalf("renewed fencing token = %d, want %d", renewed.FencingToken, first.FencingToken)
	}

	clock.Advance(11 * time.Second)
	second, err := registry.Acquire("region-a/0", "worker-b")
	if err != nil {
		t.Fatalf("Acquire(after expiry) error = %v", err)
	}
	if second.FencingToken != 2 {
		t.Fatalf("takeover fencing token = %d, want 2", second.FencingToken)
	}
	if _, err := registry.Renew(first); !errors.Is(err, hatSql.ErrSQLShardLeaseStale) {
		t.Fatalf("stale Renew error = %v, want ErrSQLShardLeaseStale", err)
	}
	if _, err := registry.Release(first); !errors.Is(err, hatSql.ErrSQLShardLeaseStale) {
		t.Fatalf("stale Release error = %v, want ErrSQLShardLeaseStale", err)
	}

	encoded, err := registry.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored := m225Registry(t, clock)
	snapshot, err := hatSql.UnmarshalSQLShardLeaseSnapshot(encoded)
	if err != nil {
		t.Fatalf("UnmarshalSQLShardLeaseSnapshot() error = %v", err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	current, ok := restored.Get("region-a/0")
	if !ok || current.Owner != "worker-b" || current.FencingToken != 2 {
		t.Fatalf("restored lease = %#v, found=%v", current, ok)
	}
	if _, err := restored.Release(current); err != nil {
		t.Fatalf("Release(restored) error = %v", err)
	}
	third, err := restored.Acquire("region-a/0", "worker-c")
	if err != nil {
		t.Fatalf("Acquire(after restore) error = %v", err)
	}
	if third.FencingToken != 3 {
		t.Fatalf("post-restore fencing token = %d, want 3", third.FencingToken)
	}
}

func TestM225ShardLeaseSnapshotsAreDeterministicAndBounded(t *testing.T) {
	clock := &m225Clock{now: time.Unix(1_700_000_000, 0).UTC()}
	registry := m225Registry(t, clock)
	if _, err := registry.Acquire("region-b/0", "worker-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("region-a/0", "worker-a"); err != nil {
		t.Fatal(err)
	}

	snapshot := registry.Snapshot()
	if len(snapshot.Leases) != 2 {
		t.Fatalf("snapshot leases = %d, want 2", len(snapshot.Leases))
	}
	if snapshot.Leases[0].ShardID != "region-a/0" || snapshot.Leases[1].ShardID != "region-b/0" {
		t.Fatalf("snapshot order = %#v", snapshot.Leases)
	}
	first, err := registry.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("MarshalBinary() is not deterministic")
	}
	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-5] ^= 1
	if _, err := hatSql.UnmarshalSQLShardLeaseSnapshot(tampered); !errors.Is(err, hatSql.ErrSQLShardLeaseSnapshotInvalid) {
		t.Fatalf("tampered snapshot error = %v, want ErrSQLShardLeaseSnapshotInvalid", err)
	}

	if _, err := registry.Acquire("", "worker"); !errors.Is(err, hatSql.ErrSQLShardLeaseShardRequired) {
		t.Fatalf("empty shard error = %v", err)
	}
	if _, err := registry.Acquire("region-c/0", ""); !errors.Is(err, hatSql.ErrSQLShardLeaseOwnerRequired) {
		t.Fatalf("empty owner error = %v", err)
	}
	if _, err := hatSql.UnmarshalSQLShardLeaseSnapshot([]byte("HSL1")); !errors.Is(err, hatSql.ErrSQLShardLeaseSnapshotInvalid) {
		t.Fatalf("truncated snapshot error = %v", err)
	}
}

func TestM225ShardLeaseConcurrentAcquireHasOneOwner(t *testing.T) {
	clock := &m225Clock{now: time.Unix(1_700_000_000, 0).UTC()}
	registry := m225Registry(t, clock)
	const callers = 32
	start := make(chan struct{})
	results := make(chan struct {
		lease hatSql.SQLShardLease
		err   error
	}, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for index := 0; index < callers; index++ {
		go func(index int) {
			defer group.Done()
			<-start
			lease, err := registry.Acquire("region-a/0", "worker-"+string(rune('a'+index)))
			results <- struct {
				lease hatSql.SQLShardLease
				err   error
			}{lease: lease, err: err}
		}(index)
	}
	close(start)
	group.Wait()
	close(results)

	acquired := 0
	for result := range results {
		if result.err == nil {
			acquired++
			if _, err := registry.Release(result.lease); err != nil {
				t.Fatalf("Release(winner) error = %v", err)
			}
			continue
		}
		if !errors.Is(result.err, hatSql.ErrSQLShardLeaseHeld) {
			t.Fatalf("contended Acquire error = %v", result.err)
		}
	}
	if acquired != 1 {
		t.Fatalf("acquired owners = %d, want 1", acquired)
	}
}

func TestM225ShardLeaseCapacityReclaimsExpiredRecords(t *testing.T) {
	clock := &m225Clock{now: time.Unix(1_700_000_000, 0).UTC()}
	registry, err := hatSql.NewSQLShardLeaseRegistry(hatSql.SQLShardLeaseRegistryOptions{
		ShardCount:    2,
		MaxShards:     1,
		LeaseDuration: time.Second,
		Now:           clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("region-a/0", "worker-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("region-b/0", "worker-b"); !errors.Is(err, hatSql.ErrSQLShardLeaseCapacity) {
		t.Fatalf("full registry error = %v, want ErrSQLShardLeaseCapacity", err)
	}
	clock.Advance(2 * time.Second)
	if _, err := registry.Acquire("region-b/0", "worker-b"); err != nil {
		t.Fatalf("Acquire(after capacity expiry) error = %v", err)
	}
}
