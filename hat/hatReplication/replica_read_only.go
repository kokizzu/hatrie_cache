package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// ErrReplicaReadOnly reports that a public mutation was attempted while a
// replica-wide read-only gate was active.
var ErrReplicaReadOnly = errors.New("hatriecache: replica is read-only")

// ReplicaReadOnlyStatus is the operator-visible state of a gate.
type ReplicaReadOnlyStatus struct {
	ReadOnly   bool   `json:"read_only"`
	Generation uint64 `json:"generation"`
	Reason     string `json:"reason,omitempty"`
}

// ReplicaReadOnlyGate rejects public writes while allowing reads to continue.
// A nil gate, or a newly created gate, is writable by default.
type ReplicaReadOnlyGate struct {
	readOnly   atomic.Bool
	generation atomic.Uint64
	reason     atomic.Pointer[string]
}

// NewReplicaReadOnlyGate returns a writable gate ready for operator control.
func NewReplicaReadOnlyGate() *ReplicaReadOnlyGate {
	return &ReplicaReadOnlyGate{}
}

// Check returns ErrReplicaReadOnly when writes are currently blocked.
func (gate *ReplicaReadOnlyGate) Check() error {
	if gate == nil || !gate.readOnly.Load() {
		return nil
	}
	reason := gate.reason.Load()
	if reason == nil || *reason == "" {
		return ErrReplicaReadOnly
	}
	return fmt.Errorf("%w: %s", ErrReplicaReadOnly, *reason)
}

// ReadOnly reports whether the gate currently rejects public writes.
func (gate *ReplicaReadOnlyGate) ReadOnly() bool {
	return gate != nil && gate.readOnly.Load()
}

// SetReadOnly blocks public writes and records an optional operator reason.
func (gate *ReplicaReadOnlyGate) SetReadOnly(reason string) ReplicaReadOnlyStatus {
	if gate == nil {
		return ReplicaReadOnlyStatus{}
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		gate.reason.Store(nil)
	} else {
		gate.reason.Store(&reason)
	}
	gate.generation.Add(1)
	gate.readOnly.Store(true)
	return gate.Status()
}

// SetWritable permits public writes again and clears the operator reason.
func (gate *ReplicaReadOnlyGate) SetWritable() ReplicaReadOnlyStatus {
	if gate == nil {
		return ReplicaReadOnlyStatus{}
	}
	gate.readOnly.Store(false)
	gate.reason.Store(nil)
	gate.generation.Add(1)
	return gate.Status()
}

// Status returns a snapshot suitable for monitoring or an operator response.
func (gate *ReplicaReadOnlyGate) Status() ReplicaReadOnlyStatus {
	if gate == nil {
		return ReplicaReadOnlyStatus{}
	}
	status := ReplicaReadOnlyStatus{
		ReadOnly:   gate.readOnly.Load(),
		Generation: gate.generation.Load(),
	}
	if reason := gate.reason.Load(); reason != nil {
		status.Reason = *reason
	}
	return status
}
