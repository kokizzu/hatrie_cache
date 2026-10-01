package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestTU06ReplicaReadOnlyGateBlocksLocalWritesAndDrainsLeases(t *testing.T) {
	gate, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{AllowOperatorOverride: true})
	if err != nil {
		t.Fatalf("new gate: %v", err)
	}

	lease, err := gate.Begin(ReplicaMutationOriginLocal)
	if err != nil {
		t.Fatalf("writable local begin: %v", err)
	}
	if state := gate.Snapshot(); state.ReadOnly || state.Generation != 0 {
		t.Fatalf("unexpected default state: %+v", state)
	}

	setDone := make(chan error, 1)
	go func() {
		_, setErr := gate.SetReadOnly("planned failover")
		setDone <- setErr
	}()
	select {
	case err := <-setDone:
		t.Fatalf("read-only transition completed while lease was held: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	lease.Release()
	select {
	case err := <-setDone:
		if err != nil {
			t.Fatalf("set read-only: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read-only transition did not drain after lease release")
	}

	if _, err := gate.Begin(ReplicaMutationOriginLocal); !errors.Is(err, ErrReplicaReadOnlyGateLocalWriteDenied) {
		t.Fatalf("local begin error = %v, want local-write-denied", err)
	}
	replicationLease, err := gate.Begin(ReplicaMutationOriginReplication)
	if err != nil {
		t.Fatalf("replication begin: %v", err)
	}
	replicationLease.Release()
	operatorLease, err := gate.Begin(ReplicaMutationOriginOperator)
	if err != nil {
		t.Fatalf("operator override begin: %v", err)
	}
	operatorLease.Release()

	state := gate.Snapshot()
	if !state.ReadOnly || state.Generation != 1 || state.Reason != "planned failover" {
		t.Fatalf("unexpected read-only state: %+v", state)
	}
	if _, err := gate.SetReadOnly("planned failover"); err != nil {
		t.Fatalf("idempotent read-only transition: %v", err)
	}
	if state := gate.Snapshot(); state.Generation != 1 {
		t.Fatalf("idempotent transition changed generation: %+v", state)
	}
	state = gate.SetWritable()
	if state.ReadOnly || state.Generation != 2 || state.Reason != "" {
		t.Fatalf("unexpected writable state: %+v", state)
	}
	lease, err = gate.Begin(ReplicaMutationOriginLocal)
	if err != nil {
		t.Fatalf("local begin after writable transition: %v", err)
	}
	lease.Release()
	lease.Release()
}

func TestTU06ReplicaReadOnlyGateValidatesOriginsAndDefaults(t *testing.T) {
	gate, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{InitiallyReadOnly: true, InitialReason: "startup recovery", DisableReplicationWrites: true})
	if err != nil {
		t.Fatalf("new initially read-only gate: %v", err)
	}
	if _, err := gate.Begin(ReplicaMutationOriginReplication); !errors.Is(err, ErrReplicaReadOnlyGateReplicationDenied) {
		t.Fatalf("replication begin error = %v, want replication-denied", err)
	}
	if _, err := gate.Begin(ReplicaMutationOriginOperator); !errors.Is(err, ErrReplicaReadOnlyGateOperatorOverrideDenied) {
		t.Fatalf("operator begin error = %v, want operator-override-denied", err)
	}
	if _, err := gate.Begin(ReplicaMutationOrigin(99)); !errors.Is(err, ErrReplicaReadOnlyGateInvalidOrigin) {
		t.Fatalf("invalid origin error = %v, want invalid-origin", err)
	}
	if _, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{InitiallyReadOnly: true}); !errors.Is(err, ErrReplicaReadOnlyGateInvalidReason) {
		t.Fatalf("missing initial reason error = %v, want invalid-reason", err)
	}
	if _, err := gate.SetReadOnly(" bad reason"); !errors.Is(err, ErrReplicaReadOnlyGateInvalidReason) {
		t.Fatalf("invalid reason error = %v, want invalid-reason", err)
	}
	if err := gate.SetWritableWithReason(" "); !errors.Is(err, ErrReplicaReadOnlyGateInvalidReason) {
		t.Fatalf("invalid writable reason error = %v, want invalid-reason", err)
	}
	if _, err := (*ReplicaReadOnlyGate)(nil).Begin(ReplicaMutationOriginLocal); !errors.Is(err, ErrReplicaReadOnlyGateNil) {
		t.Fatalf("nil begin error = %v, want nil", err)
	}
}

func TestTU06ReplicaReadOnlyGateInitialStateAndGeneration(t *testing.T) {
	gate, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{
		InitiallyReadOnly:        true,
		InitialReason:            "bootstrap",
		DisableReplicationWrites: true,
		AllowOperatorOverride:    true,
	})
	if err != nil {
		t.Fatalf("new initially read-only gate: %v", err)
	}
	state := gate.Snapshot()
	if !state.ReadOnly || state.Generation != 1 || state.Reason != "bootstrap" {
		t.Fatalf("initial state = %+v, want read-only generation 1", state)
	}
	operatorLease, err := gate.Begin(ReplicaMutationOriginOperator)
	if err != nil {
		t.Fatalf("operator override begin: %v", err)
	}
	operatorLease.Release()

	state, err = gate.SetReadOnly("bootstrap")
	if err != nil || state.Generation != 1 {
		t.Fatalf("idempotent SetReadOnly() = %+v, %v; want unchanged generation", state, err)
	}
	state, err = gate.SetReadOnly("recovery")
	if err != nil || state.Generation != 2 || state.Reason != "recovery" {
		t.Fatalf("reason transition = %+v, %v; want generation 2", state, err)
	}
	if err := gate.SetWritableWithReason("operator completed"); err != nil {
		t.Fatalf("SetWritableWithReason() error = %v", err)
	}
	if state := gate.Snapshot(); state.ReadOnly || state.Generation != 3 || state.Reason != "" {
		t.Fatalf("writable state = %+v, want generation 3 with empty reason", state)
	}
}

func TestTU06ReplicaReadOnlyGateRejectsInvalidInitialReason(t *testing.T) {
	if _, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{
		InitiallyReadOnly: true,
		InitialReason:     " bad",
	}); !errors.Is(err, ErrReplicaReadOnlyGateInvalidReason) {
		t.Fatalf("invalid initial reason error = %v, want invalid-reason", err)
	}
	if _, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{
		InitiallyReadOnly: true,
		InitialReason:     "bad\x00reason",
	}); !errors.Is(err, ErrReplicaReadOnlyGateInvalidReason) {
		t.Fatalf("control character error = %v, want invalid-reason", err)
	}
}

func BenchmarkTU06ReplicaReadOnlyGate(b *testing.B) {
	b.Run("direct_writable_flag", func(b *testing.B) {
		b.ReportAllocs()
		var allowed bool
		readOnly := false
		for i := 0; i < b.N; i++ {
			allowed = !readOnly
		}
		if !allowed {
			b.Fatal("unexpected direct write allowance")
		}
	})
	b.Run("gate_writable_lease", func(b *testing.B) {
		gate, err := NewReplicaReadOnlyGate(ReplicaReadOnlyGateOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			lease, err := gate.Begin(ReplicaMutationOriginLocal)
			if err != nil {
				b.Fatal(err)
			}
			lease.Release()
		}
	})
}
