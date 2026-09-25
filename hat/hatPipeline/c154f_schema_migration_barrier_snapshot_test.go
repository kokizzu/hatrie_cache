package hatPipeline

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestC154fSchemaMigrationBarrierSnapshotRoundTripAndAtomicRestore(t *testing.T) {
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 4, MaxDependencies: 4})
	if err != nil {
		t.Fatalf("NewSchemaMigrationBarrier() error = %v", err)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "orders-v2", Version: 2, Dependencies: []string{"reader", "sink"}}); err != nil {
		t.Fatalf("Prepare(orders-v2) error = %v", err)
	}
	if err := barrier.Acknowledge("orders-v2", "reader", 2); err != nil {
		t.Fatalf("Acknowledge() error = %v", err)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "orders-v1", Version: 1, Dependencies: []string{"reader"}}); err != nil {
		t.Fatalf("Prepare(orders-v1) error = %v", err)
	}
	if _, err := barrier.Abort("orders-v1", 1); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}

	payload, err := barrier.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	if !bytes.HasPrefix(payload, []byte("SMB1")) {
		t.Fatalf("snapshot magic = %q, want SMB1", payload[:minC154f(len(payload), 4)])
	}

	restored, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 4, MaxDependencies: 4})
	if err != nil {
		t.Fatalf("restore target constructor error = %v", err)
	}
	if err := restored.RestoreSnapshot(payload); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if got, want := restored.Snapshot(), barrier.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored snapshot = %#v, want %#v", got, want)
	}

	before := restored.Snapshot()
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := restored.RestoreSnapshot(corrupt); !errors.Is(err, ErrSchemaMigrationBarrierSnapshotChecksum) {
		t.Fatalf("corrupt snapshot error = %v, want checksum error", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed after corrupt restore = %#v, want %#v", got, before)
	}
}

func TestC154fSchemaMigrationBarrierSnapshotRejectsBoundsAndState(t *testing.T) {
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 1, MaxDependencies: 1})
	if err != nil {
		t.Fatalf("NewSchemaMigrationBarrier() error = %v", err)
	}
	for _, payload := range [][]byte{nil, []byte("wrong"), []byte("SMB1\x00\x00\x00\x00\x00\x00\x00\x00")} {
		if err := barrier.RestoreSnapshot(payload); !errors.Is(err, ErrSchemaMigrationBarrierSnapshotInvalid) && !errors.Is(err, ErrSchemaMigrationBarrierSnapshotChecksum) {
			t.Fatalf("payload %q error = %v, want snapshot validation error", payload, err)
		}
	}
}

func c154fBarrierBenchmarkFixture(t testing.TB) *SchemaMigrationBarrier {
	t.Helper()
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 64, MaxDependencies: 8})
	if err != nil {
		t.Fatalf("NewSchemaMigrationBarrier() error = %v", err)
	}
	for index := 0; index < 32; index++ {
		id := "migration-" + string(rune('a'+index))
		if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{
			ID:           id,
			Version:      uint64(index + 1),
			Dependencies: []string{"cache", "index", "reader", "sink"},
		}); err != nil {
			t.Fatalf("Prepare(%s) error = %v", id, err)
		}
		if index%3 == 0 {
			if err := barrier.Acknowledge(id, "reader", uint64(index+1)); err != nil {
				t.Fatalf("Acknowledge(%s) error = %v", id, err)
			}
		} else if index%3 == 1 {
			for _, dependency := range []string{"cache", "index", "reader", "sink"} {
				if err := barrier.Acknowledge(id, dependency, uint64(index+1)); err != nil {
					t.Fatalf("Acknowledge(%s,%s) error = %v", id, dependency, err)
				}
			}
			if _, err := barrier.Commit(id, uint64(index+1)); err != nil {
				t.Fatalf("Commit(%s) error = %v", id, err)
			}
		} else {
			if _, err := barrier.Abort(id, uint64(index+1)); err != nil {
				t.Fatalf("Abort(%s) error = %v", id, err)
			}
		}
	}
	return barrier
}

func minC154f(left, right int) int {
	if left < right {
		return left
	}
	return right
}
