package hatReplication

import (
	"errors"
	"testing"
)

func TestReplicaReadOnlyGateTransitions(t *testing.T) {
	gate := NewReplicaReadOnlyGate()
	if err := gate.Check(); err != nil {
		t.Fatalf("new gate Check() error = %v", err)
	}

	blocked := gate.SetReadOnly("planned maintenance")
	if !blocked.ReadOnly || blocked.Generation != 1 || blocked.Reason != "planned maintenance" {
		t.Fatalf("SetReadOnly() status = %#v", blocked)
	}
	if err := gate.Check(); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("blocked Check() error = %v, want ErrReplicaReadOnly", err)
	}

	writable := gate.SetWritable()
	if writable.ReadOnly || writable.Generation != 2 || writable.Reason != "" {
		t.Fatalf("SetWritable() status = %#v", writable)
	}
	if err := gate.Check(); err != nil {
		t.Fatalf("writable Check() error = %v", err)
	}
}
