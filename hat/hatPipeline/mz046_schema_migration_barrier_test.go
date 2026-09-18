package hatPipeline

import (
	"errors"
	"testing"
)

func TestMZ046SchemaMigrationBarrierCommitRequiresAllDependencies(t *testing.T) {
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 4, MaxDependencies: 4})
	if err != nil {
		t.Fatalf("NewSchemaMigrationBarrier() error = %v", err)
	}
	prepared, err := barrier.Prepare(SchemaMigrationBarrierSpec{
		ID:           "orders-v2",
		Version:      2,
		Dependencies: []string{"sink", "reader", "reader"},
	})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.State != SchemaMigrationBarrierPrepared || prepared.Dependencies == nil || len(prepared.Dependencies) != 2 {
		t.Fatalf("prepared status = %#v, want normalized prepared dependencies", prepared)
	}
	if err := barrier.Acknowledge("orders-v2", "reader", 2); err != nil {
		t.Fatalf("Acknowledge(reader) error = %v", err)
	}
	if _, err := barrier.Commit("orders-v2", 2); !errors.Is(err, ErrSchemaMigrationBarrierNotReady) {
		t.Fatalf("Commit() error = %v, want %v", err, ErrSchemaMigrationBarrierNotReady)
	}
	acknowledged, err := barrier.AcknowledgeStatus("orders-v2", "reader", 2)
	if err != nil || acknowledged.Acknowledged != 1 {
		t.Fatalf("duplicate Acknowledge() = %#v/%v, want one acknowledgement", acknowledged, err)
	}
	if err := barrier.Acknowledge("orders-v2", "sink", 1); !errors.Is(err, ErrSchemaMigrationBarrierVersionMismatch) {
		t.Fatalf("wrong-version Acknowledge() error = %v, want %v", err, ErrSchemaMigrationBarrierVersionMismatch)
	}
	if err := barrier.Acknowledge("orders-v2", "sink", 2); err != nil {
		t.Fatalf("Acknowledge(sink) error = %v", err)
	}
	committed, err := barrier.Commit("orders-v2", 2)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if committed.State != SchemaMigrationBarrierCommitted || committed.Acknowledged != 2 || committed.Remaining != 0 {
		t.Fatalf("committed status = %#v, want committed with all dependencies", committed)
	}
	if _, err := barrier.Commit("orders-v2", 2); err != nil {
		t.Fatalf("idempotent Commit() error = %v", err)
	}
}

func TestMZ046SchemaMigrationBarrierBoundsAndTerminalLifecycle(t *testing.T) {
	barrier, err := NewSchemaMigrationBarrier(SchemaMigrationBarrierOptions{MaxBarriers: 1, MaxDependencies: 1})
	if err != nil {
		t.Fatalf("NewSchemaMigrationBarrier() error = %v", err)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "", Version: 1, Dependencies: []string{"reader"}}); !errors.Is(err, ErrSchemaMigrationBarrierIDEmpty) {
		t.Fatalf("empty ID error = %v, want %v", err, ErrSchemaMigrationBarrierIDEmpty)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "orders-v2", Version: 1, Dependencies: []string{"reader", "sink"}}); !errors.Is(err, ErrSchemaMigrationBarrierDependencyLimit) {
		t.Fatalf("dependency limit error = %v, want %v", err, ErrSchemaMigrationBarrierDependencyLimit)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "orders-v2", Version: 1, Dependencies: []string{"reader"}}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := barrier.Prepare(SchemaMigrationBarrierSpec{ID: "orders-v3", Version: 2, Dependencies: []string{"reader"}}); !errors.Is(err, ErrSchemaMigrationBarrierCapacity) {
		t.Fatalf("capacity error = %v, want %v", err, ErrSchemaMigrationBarrierCapacity)
	}
	if _, err := barrier.Abort("orders-v2", 1); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if err := barrier.Acknowledge("orders-v2", "reader", 1); !errors.Is(err, ErrSchemaMigrationBarrierTerminal) {
		t.Fatalf("terminal Acknowledge() error = %v, want %v", err, ErrSchemaMigrationBarrierTerminal)
	}
	if err := barrier.Forget("orders-v2"); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, ok := barrier.Status("orders-v2"); ok {
		t.Fatal("Status() found forgotten barrier")
	}
}
