package hatReplication_test

import (
	"errors"
	"sync"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestTU06ReplicaWriteGateDefaultsWritable(t *testing.T) {
	gate, err := hatReplication.NewReplicaWriteGate(hatReplication.ReplicaWriteGateOptions{})
	if err != nil {
		t.Fatalf("NewReplicaWriteGate() error = %v", err)
	}
	if err := gate.Admit(hatReplication.ReplicaWriteExternal); err != nil {
		t.Fatalf("default external write error = %v", err)
	}
	state := gate.Snapshot()
	if state.ReadOnly || state.Generation != 0 || state.Reason != "" {
		t.Fatalf("default state = %#v, want writable generation zero", state)
	}
}

func TestTU06ReplicaWriteGateBlocksExternalAndAllowsExplicitExceptions(t *testing.T) {
	token := []byte("0123456789abcdef")
	gate, err := hatReplication.NewReplicaWriteGate(hatReplication.ReplicaWriteGateOptions{
		AllowOperatorOverride: true,
		OperatorOverrideToken: token,
	})
	if err != nil {
		t.Fatalf("NewReplicaWriteGate() error = %v", err)
	}
	state, err := gate.SetReadOnly("planned maintenance")
	if err != nil {
		t.Fatalf("SetReadOnly() error = %v", err)
	}
	if !state.ReadOnly || state.Generation != 1 || state.Reason != "planned maintenance" || state.ChangedAtUnixNano == 0 {
		t.Fatalf("read-only state = %#v", state)
	}
	if err := gate.Admit(hatReplication.ReplicaWriteExternal); !errors.Is(err, hatReplication.ErrReplicaWriteBlocked) {
		t.Fatalf("external admission error = %v, want blocked", err)
	}
	if err := gate.Admit(hatReplication.ReplicaWriteInternalReplication); err != nil {
		t.Fatalf("internal replication admission error = %v", err)
	}
	if err := gate.AdmitOperatorOverride([]byte("wrong-token")); !errors.Is(err, hatReplication.ErrReplicaWriteOverrideInvalid) {
		t.Fatalf("wrong override error = %v, want invalid override", err)
	}
	if err := gate.AdmitOperatorOverride(token); err != nil {
		t.Fatalf("valid override error = %v", err)
	}
	if _, err := gate.SetWritable(0); !errors.Is(err, hatReplication.ErrReplicaWriteGenerationMismatch) {
		t.Fatalf("stale SetWritable() error = %v, want generation mismatch", err)
	}
	state, err = gate.SetWritable(state.Generation)
	if err != nil {
		t.Fatalf("SetWritable() error = %v", err)
	}
	if state.ReadOnly || state.Generation != 2 {
		t.Fatalf("writable state = %#v", state)
	}
}

func TestTU06ReplicaWriteGateValidatesOptionsAndState(t *testing.T) {
	tests := []hatReplication.ReplicaWriteGateOptions{
		{AllowOperatorOverride: true},
		{AllowOperatorOverride: true, OperatorOverrideToken: []byte("short")},
		{OperatorOverrideToken: make([]byte, 129)},
	}
	for _, options := range tests {
		if _, err := hatReplication.NewReplicaWriteGate(options); !errors.Is(err, hatReplication.ErrReplicaWriteGateOptionsInvalid) {
			t.Fatalf("options %#v error = %v, want invalid options", options, err)
		}
	}
	gate, err := hatReplication.NewReplicaWriteGate(hatReplication.ReplicaWriteGateOptions{})
	if err != nil {
		t.Fatalf("NewReplicaWriteGate() error = %v", err)
	}
	if _, err := gate.SetReadOnly(""); !errors.Is(err, hatReplication.ErrReplicaWriteStateInvalid) {
		t.Fatalf("empty reason error = %v, want invalid state", err)
	}
	if err := gate.Admit(hatReplication.ReplicaWriteOrigin(99)); !errors.Is(err, hatReplication.ErrReplicaWriteOriginInvalid) {
		t.Fatalf("invalid origin error = %v, want invalid origin", err)
	}
	if err := gate.AdmitOperatorOverride([]byte("anything")); !errors.Is(err, hatReplication.ErrReplicaWriteOverrideDisabled) {
		t.Fatalf("disabled override error = %v, want disabled override", err)
	}
}

func TestTU06ReplicaWriteGateConcurrentAdmissionsAndTransitions(t *testing.T) {
	gate, err := hatReplication.NewReplicaWriteGate(hatReplication.ReplicaWriteGateOptions{})
	if err != nil {
		t.Fatalf("NewReplicaWriteGate() error = %v", err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 100; index++ {
				if err := gate.Admit(hatReplication.ReplicaWriteExternal); err != nil && !errors.Is(err, hatReplication.ErrReplicaWriteBlocked) {
					t.Errorf("admission error = %v", err)
					return
				}
				_ = gate.Snapshot()
			}
		}()
	}
	state, err := gate.SetReadOnly("test")
	if err != nil {
		t.Fatalf("SetReadOnly() error = %v", err)
	}
	group.Add(1)
	go func() {
		defer group.Done()
		_, _ = gate.SetWritable(state.Generation)
	}()
	group.Wait()
}
