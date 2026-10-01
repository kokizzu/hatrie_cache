package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

const maxReplicaReadOnlyReasonBytes = 256

var (
	ErrReplicaReadOnlyGateNil                    = errors.New("hatrie_cache: replica read-only gate is nil")
	ErrReplicaReadOnlyGateInvalidOrigin          = errors.New("hatrie_cache: replica read-only gate origin is invalid")
	ErrReplicaReadOnlyGateInvalidReason          = errors.New("hatrie_cache: replica read-only gate reason is invalid")
	ErrReplicaReadOnlyGateLocalWriteDenied       = errors.New("hatrie_cache: local write denied by replica read-only gate")
	ErrReplicaReadOnlyGateReplicationDenied      = errors.New("hatrie_cache: replication write denied by replica read-only gate")
	ErrReplicaReadOnlyGateOperatorOverrideDenied = errors.New("hatrie_cache: operator override denied by replica read-only gate")
	ErrReplicaReadOnlyGateGenerationExhausted    = errors.New("hatrie_cache: replica read-only gate generation exhausted")
)

// ReplicaMutationOrigin identifies the caller that wants to mutate a replica.
type ReplicaMutationOrigin uint8

const (
	ReplicaMutationOriginLocal ReplicaMutationOrigin = iota + 1
	ReplicaMutationOriginReplication
	ReplicaMutationOriginOperator
)

// ReplicaReadOnlyGateOptions configures a replica read-only gate.
//
// The zero value keeps the gate writable. Replication writes remain allowed
// during read-only mode unless DisableReplicationWrites is set.
type ReplicaReadOnlyGateOptions struct {
	InitiallyReadOnly        bool
	InitialReason            string
	DisableReplicationWrites bool
	AllowOperatorOverride    bool
}

// ReplicaReadOnlyState is a point-in-time copy of the gate state.
type ReplicaReadOnlyState struct {
	ReadOnly   bool
	Generation uint64
	Reason     string
}

// ReplicaReadOnlyGate coordinates caller-owned mutation paths on a replica.
//
// Begin returns a lease that holds a read lock until Release. A read-only
// transition takes the write lock, so it waits for already admitted leases to
// finish before any later mutation can begin. The gate does not discover or
// intercept direct HatTrie mutations automatically; callers must acquire and
// release a lease around each path they want protected.
type ReplicaReadOnlyGate struct {
	mu sync.RWMutex

	readOnly                 bool
	generation               uint64
	reason                   string
	disableReplicationWrites bool
	allowOperatorOverride    bool
	initErr                  error
}

// NewReplicaReadOnlyGate constructs a gate. Invalid initial configuration is
// returned as an error instead of causing a panic.
func NewReplicaReadOnlyGate(options ReplicaReadOnlyGateOptions) (*ReplicaReadOnlyGate, error) {
	gate := &ReplicaReadOnlyGate{
		disableReplicationWrites: options.DisableReplicationWrites,
		allowOperatorOverride:    options.AllowOperatorOverride,
	}

	if options.InitialReason != "" || options.InitiallyReadOnly {
		if err := validateReplicaReadOnlyReason(options.InitialReason, true); err != nil {
			return nil, fmt.Errorf("%w: initial reason: %v", ErrReplicaReadOnlyGateInvalidReason, err)
		}
	}
	if options.InitiallyReadOnly {
		gate.readOnly = true
		gate.generation = 1
		gate.reason = options.InitialReason
	}
	return gate, nil
}

// Validate reports an invalid initial configuration, if any.
func (gate *ReplicaReadOnlyGate) Validate() error {
	if gate == nil {
		return ErrReplicaReadOnlyGateNil
	}
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	return gate.initErr
}

// Begin admits a mutation origin and returns a lease that must be released.
func (gate *ReplicaReadOnlyGate) Begin(origin ReplicaMutationOrigin) (*ReplicaMutationLease, error) {
	if gate == nil {
		return nil, ErrReplicaReadOnlyGateNil
	}
	if !validReplicaMutationOrigin(origin) {
		return nil, ErrReplicaReadOnlyGateInvalidOrigin
	}

	gate.mu.RLock()
	if gate.initErr != nil {
		gate.mu.RUnlock()
		return nil, gate.initErr
	}
	if gate.readOnly {
		var err error
		switch origin {
		case ReplicaMutationOriginLocal:
			err = ErrReplicaReadOnlyGateLocalWriteDenied
		case ReplicaMutationOriginReplication:
			if gate.disableReplicationWrites {
				err = ErrReplicaReadOnlyGateReplicationDenied
			}
		case ReplicaMutationOriginOperator:
			if !gate.allowOperatorOverride {
				err = ErrReplicaReadOnlyGateOperatorOverrideDenied
			}
		}
		if err != nil {
			gate.mu.RUnlock()
			return nil, err
		}
	}
	return &ReplicaMutationLease{gate: gate}, nil
}

// Snapshot returns the current gate state. A nil gate returns the zero state.
func (gate *ReplicaReadOnlyGate) Snapshot() ReplicaReadOnlyState {
	if gate == nil {
		return ReplicaReadOnlyState{}
	}
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	return gate.snapshotLocked()
}

// SetReadOnly enters read-only mode after all admitted leases have released.
// Repeating the same state and reason is idempotent.
func (gate *ReplicaReadOnlyGate) SetReadOnly(reason string) (ReplicaReadOnlyState, error) {
	if gate == nil {
		return ReplicaReadOnlyState{}, ErrReplicaReadOnlyGateNil
	}
	if err := validateReplicaReadOnlyReason(reason, true); err != nil {
		return gate.Snapshot(), err
	}

	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.initErr != nil {
		return gate.snapshotLocked(), gate.initErr
	}
	if gate.readOnly && gate.reason == reason {
		return gate.snapshotLocked(), nil
	}
	if err := gate.advanceGenerationLocked(); err != nil {
		return gate.snapshotLocked(), err
	}
	gate.readOnly = true
	gate.reason = reason
	return gate.snapshotLocked(), nil
}

// SetWritable leaves read-only mode and clears the visible reason. The
// practical generation limit is unreachable for a live process; callers that
// need the error should use SetWritableWithReason.
func (gate *ReplicaReadOnlyGate) SetWritable() ReplicaReadOnlyState {
	if gate == nil {
		return ReplicaReadOnlyState{}
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	state, _ := gate.setWritableLocked()
	return state
}

func (gate *ReplicaReadOnlyGate) setWritableLocked() (ReplicaReadOnlyState, error) {
	if gate.initErr != nil || !gate.readOnly {
		return gate.snapshotLocked(), gate.initErr
	}
	if err := gate.advanceGenerationLocked(); err != nil {
		return gate.snapshotLocked(), err
	}
	gate.readOnly = false
	gate.reason = ""
	return gate.snapshotLocked(), nil
}

// SetWritableWithReason validates an optional operator transition reason and
// then leaves read-only mode. The current state intentionally exposes no
// reason while writable; the argument is a validation hook for callers that
// record the operator event separately.
func (gate *ReplicaReadOnlyGate) SetWritableWithReason(reason string) error {
	if gate == nil {
		return ErrReplicaReadOnlyGateNil
	}
	if reason != "" {
		if err := validateReplicaReadOnlyReason(reason, true); err != nil {
			return err
		}
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	_, err := gate.setWritableLocked()
	return err
}

func (gate *ReplicaReadOnlyGate) advanceGenerationLocked() error {
	if gate.generation == ^uint64(0) {
		return ErrReplicaReadOnlyGateGenerationExhausted
	}
	gate.generation++
	return nil
}

func (gate *ReplicaReadOnlyGate) snapshotLocked() ReplicaReadOnlyState {
	return ReplicaReadOnlyState{
		ReadOnly:   gate.readOnly,
		Generation: gate.generation,
		Reason:     gate.reason,
	}
}

// ReplicaMutationLease keeps a mutation admitted by ReplicaReadOnlyGate.
// Release is safe to call repeatedly.
type ReplicaMutationLease struct {
	gate *ReplicaReadOnlyGate
	once sync.Once
}

// Release ends the lease. A nil lease is a no-op.
func (lease *ReplicaMutationLease) Release() {
	if lease == nil || lease.gate == nil {
		return
	}
	lease.once.Do(func() { lease.gate.mu.RUnlock() })
}

func validReplicaMutationOrigin(origin ReplicaMutationOrigin) bool {
	return origin >= ReplicaMutationOriginLocal && origin <= ReplicaMutationOriginOperator
}

func validateReplicaReadOnlyReason(reason string, required bool) error {
	if reason == "" {
		if required {
			return ErrReplicaReadOnlyGateInvalidReason
		}
		return nil
	}
	if len(reason) > maxReplicaReadOnlyReasonBytes || !utf8.ValidString(reason) {
		return ErrReplicaReadOnlyGateInvalidReason
	}
	if strings.TrimSpace(reason) != reason {
		return ErrReplicaReadOnlyGateInvalidReason
	}
	for _, character := range reason {
		if unicode.IsControl(character) {
			return ErrReplicaReadOnlyGateInvalidReason
		}
	}
	return nil
}
