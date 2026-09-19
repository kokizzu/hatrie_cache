package hatReplication

import (
	"crypto/hmac"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrReplicaWriteGateNil            = errors.New("hatriecache: replica write gate is nil")
	ErrReplicaWriteGateOptionsInvalid = errors.New("hatriecache: replica write gate options are invalid")
	ErrReplicaWriteStateInvalid       = errors.New("hatriecache: replica read-only state is invalid")
	ErrReplicaWriteOriginInvalid      = errors.New("hatriecache: replica write origin is invalid")
	ErrReplicaWriteBlocked            = errors.New("hatriecache: replica is read-only")
	ErrReplicaWriteOverrideDisabled   = errors.New("hatriecache: replica write override is disabled")
	ErrReplicaWriteOverrideInvalid    = errors.New("hatriecache: replica write override is invalid")
	ErrReplicaWriteGenerationMismatch = errors.New("hatriecache: replica write generation mismatch")
)

const (
	MinReplicaWriteOverrideTokenBytes = 16
	MaxReplicaWriteOverrideTokenBytes = 128
	MaxReplicaWriteReasonBytes        = 256
)

// ReplicaWriteOrigin identifies the caller class presented to a write gate.
// Internal replication is an explicit exception because replicas must apply
// authenticated incoming writes while rejecting local client writes.
type ReplicaWriteOrigin uint8

const (
	ReplicaWriteExternal ReplicaWriteOrigin = iota
	ReplicaWriteInternalReplication
)

// ReplicaWriteGateOptions configures a gate. The zero value is writable and
// has no operator override, preserving existing behavior.
type ReplicaWriteGateOptions struct {
	DefaultReadOnly       bool
	DefaultReason         string
	AllowOperatorOverride bool
	OperatorOverrideToken []byte
}

// ReplicaReadOnlyState is a detached read-only state snapshot. Generation
// increases on every successful state transition and prevents stale operators
// from clearing a newer maintenance decision.
type ReplicaReadOnlyState struct {
	ReadOnly          bool
	Generation        uint64
	Reason            string
	ChangedAtUnixNano int64
}

// ReplicaWriteGate is an opt-in process-local admission gate. It does not
// authenticate callers or install itself into any transport or storage API;
// the embedding service must invoke Admit at each mutation boundary.
type ReplicaWriteGate struct {
	mu                    sync.RWMutex
	state                 ReplicaReadOnlyState
	allowOperatorOverride bool
	operatorOverrideToken []byte
}

// NewReplicaWriteGate creates a default-off write gate.
func NewReplicaWriteGate(options ReplicaWriteGateOptions) (*ReplicaWriteGate, error) {
	if err := validateReplicaWriteGateOptions(options); err != nil {
		return nil, err
	}
	gate := &ReplicaWriteGate{
		allowOperatorOverride: options.AllowOperatorOverride,
		operatorOverrideToken: append([]byte(nil), options.OperatorOverrideToken...),
	}
	if options.DefaultReadOnly {
		gate.state = ReplicaReadOnlyState{
			ReadOnly:          true,
			Generation:        1,
			Reason:            strings.TrimSpace(options.DefaultReason),
			ChangedAtUnixNano: time.Now().UnixNano(),
		}
	}
	return gate, nil
}

// Admit checks whether a write origin may proceed under the current state.
// Internal replication is allowed through read-only mode, but the caller must
// authenticate and fence that origin before invoking this method.
func (gate *ReplicaWriteGate) Admit(origin ReplicaWriteOrigin) error {
	if gate == nil {
		return ErrReplicaWriteGateNil
	}
	if origin != ReplicaWriteExternal && origin != ReplicaWriteInternalReplication {
		return ErrReplicaWriteOriginInvalid
	}
	gate.mu.RLock()
	readOnly := gate.state.ReadOnly
	gate.mu.RUnlock()
	if !readOnly || origin == ReplicaWriteInternalReplication {
		return nil
	}
	return ErrReplicaWriteBlocked
}

// AdmitOperatorOverride admits a local write using the configured operator
// token. Token comparison is constant-time. Authentication and secure token
// delivery remain responsibilities of the embedding service.
func (gate *ReplicaWriteGate) AdmitOperatorOverride(token []byte) error {
	if gate == nil {
		return ErrReplicaWriteGateNil
	}
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	if !gate.allowOperatorOverride {
		return ErrReplicaWriteOverrideDisabled
	}
	if !hmac.Equal(token, gate.operatorOverrideToken) {
		return ErrReplicaWriteOverrideInvalid
	}
	return nil
}

// SetReadOnly enables the gate and returns the new generation.
func (gate *ReplicaWriteGate) SetReadOnly(reason string) (ReplicaReadOnlyState, error) {
	if gate == nil {
		return ReplicaReadOnlyState{}, ErrReplicaWriteGateNil
	}
	reason, err := normalizeReplicaWriteReason(reason)
	if err != nil {
		return ReplicaReadOnlyState{}, err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	nextGeneration, err := nextReplicaWriteGeneration(gate.state.Generation)
	if err != nil {
		return ReplicaReadOnlyState{}, err
	}
	gate.state = ReplicaReadOnlyState{
		ReadOnly:          true,
		Generation:        nextGeneration,
		Reason:            reason,
		ChangedAtUnixNano: time.Now().UnixNano(),
	}
	return gate.state, nil
}

// SetWritable clears read-only mode only when expectedGeneration is current.
// A zero-generation writable gate accepts SetWritable(0) as an idempotent
// no-op; all active read-only states require their exact generation.
func (gate *ReplicaWriteGate) SetWritable(expectedGeneration uint64) (ReplicaReadOnlyState, error) {
	if gate == nil {
		return ReplicaReadOnlyState{}, ErrReplicaWriteGateNil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if expectedGeneration != gate.state.Generation {
		return ReplicaReadOnlyState{}, ErrReplicaWriteGenerationMismatch
	}
	if !gate.state.ReadOnly {
		return gate.state, nil
	}
	nextGeneration, err := nextReplicaWriteGeneration(gate.state.Generation)
	if err != nil {
		return ReplicaReadOnlyState{}, err
	}
	gate.state = ReplicaReadOnlyState{
		Generation:        nextGeneration,
		ChangedAtUnixNano: time.Now().UnixNano(),
	}
	return gate.state, nil
}

// Snapshot returns a detached current state.
func (gate *ReplicaWriteGate) Snapshot() ReplicaReadOnlyState {
	if gate == nil {
		return ReplicaReadOnlyState{}
	}
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	return gate.state
}

func validateReplicaWriteGateOptions(options ReplicaWriteGateOptions) error {
	if options.DefaultReadOnly {
		if _, err := normalizeReplicaWriteReason(options.DefaultReason); err != nil {
			return fmt.Errorf("%w: default reason: %v", ErrReplicaWriteGateOptionsInvalid, err)
		}
	} else if options.DefaultReason != "" {
		return fmt.Errorf("%w: default reason requires DefaultReadOnly", ErrReplicaWriteGateOptionsInvalid)
	}
	if options.AllowOperatorOverride {
		if len(options.OperatorOverrideToken) < MinReplicaWriteOverrideTokenBytes || len(options.OperatorOverrideToken) > MaxReplicaWriteOverrideTokenBytes {
			return fmt.Errorf("%w: operator token must be between %d and %d bytes", ErrReplicaWriteGateOptionsInvalid, MinReplicaWriteOverrideTokenBytes, MaxReplicaWriteOverrideTokenBytes)
		}
	} else if len(options.OperatorOverrideToken) != 0 {
		return fmt.Errorf("%w: operator token requires AllowOperatorOverride", ErrReplicaWriteGateOptionsInvalid)
	}
	return nil
}

func normalizeReplicaWriteReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > MaxReplicaWriteReasonBytes || strings.IndexByte(reason, 0) >= 0 {
		return "", fmt.Errorf("%w: reason is required and must be at most %d bytes", ErrReplicaWriteStateInvalid, MaxReplicaWriteReasonBytes)
	}
	return reason, nil
}

func nextReplicaWriteGeneration(generation uint64) (uint64, error) {
	if generation == ^uint64(0) {
		return 0, fmt.Errorf("%w: generation exhausted", ErrReplicaWriteStateInvalid)
	}
	return generation + 1, nil
}
