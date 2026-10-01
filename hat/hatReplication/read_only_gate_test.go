package hatReplication

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestReadOnlyGateBlocksExternalMutationsAndAllowsIssuedReplicationPermit(t *testing.T) {
	gate, permit := NewReadOnlyGate(false)
	if gate.ReadOnly() {
		t.Fatal("new gate is read-only")
	}
	if err := gate.CheckMutation(); err != nil {
		t.Fatalf("writable mutation check returned error: %v", err)
	}
	if err := gate.CheckReplication(permit); err != nil {
		t.Fatalf("replication check returned error: %v", err)
	}

	gate.SetReadOnly(true)
	if !gate.ReadOnly() {
		t.Fatal("gate did not become read-only")
	}
	if err := gate.CheckMutation(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("mutation error = %v, want ErrReadOnly", err)
	}
	if err := gate.CheckReplication(permit); err != nil {
		t.Fatalf("replication permit was blocked: %v", err)
	}

	gate.SetReadOnly(false)
	if err := gate.CheckMutation(); err != nil {
		t.Fatalf("mutation check after enabling writes returned error: %v", err)
	}
}

func TestReadOnlyGateRejectsForgedOrCrossGateReplicationPermits(t *testing.T) {
	first, firstPermit := NewReadOnlyGate(true)
	second, _ := NewReadOnlyGate(true)
	if err := first.CheckReplication(ReplicationPermit{}); !errors.Is(err, ErrInvalidReplicationPermit) {
		t.Fatalf("zero permit error = %v, want ErrInvalidReplicationPermit", err)
	}
	if err := second.CheckReplication(firstPermit); !errors.Is(err, ErrInvalidReplicationPermit) {
		t.Fatalf("cross-gate permit error = %v, want ErrInvalidReplicationPermit", err)
	}
	if err := first.CheckReplication(firstPermit); err != nil {
		t.Fatalf("issued permit error = %v", err)
	}
	copyOfGate := *first
	if err := copyOfGate.CheckReplication(firstPermit); !errors.Is(err, ErrInvalidReplicationPermit) {
		t.Fatalf("copied-gate permit error = %v, want ErrInvalidReplicationPermit", err)
	}
}

func TestReadOnlyGateConcurrentStateChanges(t *testing.T) {
	gate, permit := NewReadOnlyGate(false)
	var blocked atomic.Int32
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 1000 {
				if err := gate.CheckMutation(); err != nil && !errors.Is(err, ErrReadOnly) {
					blocked.Add(1)
				}
				if err := gate.CheckReplication(permit); err != nil {
					blocked.Add(1)
				}
			}
		}()
	}
	for range 1000 {
		gate.SetReadOnly(true)
		gate.SetReadOnly(false)
	}
	wait.Wait()
	if blocked.Load() != 0 {
		t.Fatalf("unexpected concurrent check failures: %d", blocked.Load())
	}
}
