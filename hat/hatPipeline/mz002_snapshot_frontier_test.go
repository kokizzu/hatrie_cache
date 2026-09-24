package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ002SnapshotFrontierGateWaitsForEverySource(t *testing.T) {
	registry := newMZ002Registry(t, "orders", "payments")
	gate, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"payments", "orders"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if gate.Ready(10) {
		t.Fatal("gate is ready before any source advances")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		snapshot SnapshotFrontier
		err      error
	}, 1)
	go func() {
		snapshot, err := gate.WaitUntil(ctx, 10)
		result <- struct {
			snapshot SnapshotFrontier
			err      error
		}{snapshot: snapshot, err: err}
	}()
	assertMZ002GateStillWaiting(t, result)
	if err := registry.Advance("orders", 10, 10); err != nil {
		t.Fatal(err)
	}
	assertMZ002GateStillWaiting(t, result)
	if err := registry.Advance("payments", 10, 10); err != nil {
		t.Fatal(err)
	}

	resultValue := <-result
	if resultValue.err != nil {
		t.Fatal(resultValue.err)
	}
	snapshot := resultValue.snapshot
	if err := context.Cause(ctx); err != nil {
		t.Fatalf("wait context ended with error after success: %v", err)
	}
	if !gate.Ready(10) {
		t.Fatal("gate is not ready after every source advances")
	}
	if snapshot.Target != 10 {
		t.Fatalf("target = %d, want 10", snapshot.Target)
	}
	if len(snapshot.Sources) != 2 {
		t.Fatalf("source count = %d, want 2", len(snapshot.Sources))
	}
	if snapshot.Sources[0].ID != "orders" || snapshot.Sources[1].ID != "payments" {
		t.Fatalf("sources = %#v, want deterministic ID order", snapshot.Sources)
	}
	for _, source := range snapshot.Sources {
		if source.Lower < 10 {
			t.Fatalf("source %q lower = %d, want at least 10", source.ID, source.Lower)
		}
	}
}

func assertMZ002GateStillWaiting(t *testing.T, result <-chan struct {
	snapshot SnapshotFrontier
	err      error
}) {
	t.Helper()
	select {
	case resultValue := <-result:
		t.Fatalf("gate returned early: %#v", resultValue)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestMZ002SnapshotFrontierGateCancellation(t *testing.T) {
	registry := newMZ002Registry(t, "orders", "payments")
	gate, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"orders", "payments"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("orders", 5, 5); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = gate.WaitUntil(ctx, 10)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
}

func TestMZ002SnapshotFrontierGateValidation(t *testing.T) {
	registry := newMZ002Registry(t, "orders")

	if _, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{SourceIDs: []string{"orders"}}); !errors.Is(err, ErrSnapshotFrontierRegistryRequired) {
		t.Fatalf("nil registry error = %v", err)
	}
	if _, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{Registry: registry}); !errors.Is(err, ErrSnapshotFrontierSourcesEmpty) {
		t.Fatalf("empty sources error = %v", err)
	}
	if _, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"orders", "orders"},
	}); !errors.Is(err, ErrSnapshotFrontierSourceDuplicate) {
		t.Fatalf("duplicate source error = %v", err)
	}
	if _, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"missing"},
	}); !errors.Is(err, ErrSnapshotFrontierSourceMissing) {
		t.Fatalf("missing source error = %v", err)
	}
}

func TestMZ002SnapshotFrontierGateCopiesSnapshot(t *testing.T) {
	registry := newMZ002Registry(t, "orders")
	if err := registry.Advance("orders", 7, 9); err != nil {
		t.Fatal(err)
	}
	gate, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"orders"},
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, ok := gate.Snapshot(7)
	if !ok {
		t.Fatal("snapshot is not ready")
	}
	snapshot.Sources[0].Lower = 999
	copyOfSources := gate.Sources()
	copyOfSources[0] = "changed"

	again, ok := gate.Snapshot(7)
	if !ok {
		t.Fatal("second snapshot is not ready")
	}
	if again.Sources[0].Lower != 7 || gate.Sources()[0] != "orders" {
		t.Fatalf("gate state was mutated through returned data: %#v / %#v", again, gate.Sources())
	}
}

func newMZ002Registry(t *testing.T, ids ...string) *FrontierRegistry {
	t.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: len(ids)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	for _, id := range ids {
		if err := registry.Register(id); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}
