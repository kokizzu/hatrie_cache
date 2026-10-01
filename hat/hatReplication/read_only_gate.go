package hatReplication

import (
	"errors"
	"sync/atomic"
)

var (
	ErrReadOnly                 = errors.New("hatReplication: external mutations are disabled")
	ErrInvalidReadOnlyGate      = errors.New("hatReplication: read-only gate is invalid")
	ErrInvalidReplicationPermit = errors.New("hatReplication: replication permit is invalid")
)

// ReadOnlyGate is an opt-in atomic admission gate for external mutations. Its
// zero value is writable but has no replication permit.
type ReadOnlyGate struct {
	readOnly uint32
}

// ReplicationPermit is issued only by NewReadOnlyGate and is bound to that
// exact gate instance. It is the explicit exception for trusted appliers.
type ReplicationPermit struct {
	gate *ReadOnlyGate
}

// NewReadOnlyGate returns a gate and its private replication permit.
func NewReadOnlyGate(readOnly bool) (*ReadOnlyGate, ReplicationPermit) {
	gate := &ReadOnlyGate{}
	gate.SetReadOnly(readOnly)
	return gate, ReplicationPermit{gate: gate}
}

// SetReadOnly atomically changes external mutation admission.
func (gate *ReadOnlyGate) SetReadOnly(readOnly bool) {
	if gate == nil {
		return
	}
	if readOnly {
		atomic.StoreUint32(&gate.readOnly, 1)
		return
	}
	atomic.StoreUint32(&gate.readOnly, 0)
}

// ReadOnly reports the current external mutation state.
func (gate *ReadOnlyGate) ReadOnly() bool {
	return gate != nil && atomic.LoadUint32(&gate.readOnly) != 0
}

// CheckMutation admits an external mutation or returns ErrReadOnly.
func (gate *ReadOnlyGate) CheckMutation() error {
	if gate == nil {
		return ErrInvalidReadOnlyGate
	}
	if gate.ReadOnly() {
		return ErrReadOnly
	}
	return nil
}

// CheckReplication admits a trusted replication mutation. Read-only state
// does not block a permit issued by this exact gate.
func (gate *ReadOnlyGate) CheckReplication(permit ReplicationPermit) error {
	if gate == nil {
		return ErrInvalidReadOnlyGate
	}
	if permit.gate != gate {
		return ErrInvalidReplicationPermit
	}
	return nil
}
