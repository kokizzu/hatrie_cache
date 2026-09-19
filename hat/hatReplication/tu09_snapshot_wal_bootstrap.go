package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultSnapshotWALBootstrapMaxGap bounds the journal catch-up interval
	// accepted by a new bootstrap plan when the caller does not configure one.
	DefaultSnapshotWALBootstrapMaxGap uint64 = 1 << 20
	// MaxSnapshotWALBootstrapMaxGap prevents an accidentally unbounded plan.
	MaxSnapshotWALBootstrapMaxGap uint64 = 1 << 32
	// MaxSnapshotWALBootstrapIdentifierBytes bounds node and snapshot names.
	MaxSnapshotWALBootstrapIdentifierBytes = 256
	// MaxSnapshotWALBootstrapReasonBytes bounds operator abort reasons.
	MaxSnapshotWALBootstrapReasonBytes = 256
)

var (
	// ErrSnapshotWALBootstrapNil indicates a nil coordinator.
	ErrSnapshotWALBootstrapNil = errors.New("hatReplication: snapshot WAL bootstrap coordinator is nil")
	// ErrSnapshotWALBootstrapInvalid indicates malformed plan or transition data.
	ErrSnapshotWALBootstrapInvalid = errors.New("hatReplication: snapshot WAL bootstrap data is invalid")
	// ErrSnapshotWALBootstrapAlreadyStarted indicates that Begin was called twice.
	ErrSnapshotWALBootstrapAlreadyStarted = errors.New("hatReplication: snapshot WAL bootstrap already started")
	// ErrSnapshotWALBootstrapPhase indicates an operation is not valid in the
	// current lifecycle phase.
	ErrSnapshotWALBootstrapPhase = errors.New("hatReplication: snapshot WAL bootstrap phase does not permit the operation")
	// ErrSnapshotWALBootstrapFencing indicates a stale or missing fencing token.
	ErrSnapshotWALBootstrapFencing = errors.New("hatReplication: snapshot WAL bootstrap fencing token mismatch")
	// ErrSnapshotWALBootstrapGeneration indicates a stale lifecycle generation.
	ErrSnapshotWALBootstrapGeneration = errors.New("hatReplication: snapshot WAL bootstrap generation mismatch")
	// ErrSnapshotWALBootstrapSequence indicates invalid journal progress.
	ErrSnapshotWALBootstrapSequence = errors.New("hatReplication: snapshot WAL bootstrap journal sequence is invalid")
	// ErrSnapshotWALBootstrapLimit indicates that a plan exceeds its configured
	// journal catch-up bound.
	ErrSnapshotWALBootstrapLimit = errors.New("hatReplication: snapshot WAL bootstrap journal gap exceeds limit")
	// ErrSnapshotWALBootstrapNotReady indicates that the target journal sequence
	// has not been applied yet.
	ErrSnapshotWALBootstrapNotReady = errors.New("hatReplication: snapshot WAL bootstrap is not ready")
)

// SnapshotWALBootstrapPhase identifies the transport-neutral join lifecycle.
type SnapshotWALBootstrapPhase uint8

const (
	SnapshotWALBootstrapPhaseIdle SnapshotWALBootstrapPhase = iota
	SnapshotWALBootstrapPhaseSnapshotPending
	SnapshotWALBootstrapPhaseCatchingUp
	SnapshotWALBootstrapPhaseReady
	SnapshotWALBootstrapPhaseActive
	SnapshotWALBootstrapPhaseAborted
)

// String returns a stable phase name for status and logs.
func (phase SnapshotWALBootstrapPhase) String() string {
	switch phase {
	case SnapshotWALBootstrapPhaseIdle:
		return "idle"
	case SnapshotWALBootstrapPhaseSnapshotPending:
		return "snapshot_pending"
	case SnapshotWALBootstrapPhaseCatchingUp:
		return "catching_up"
	case SnapshotWALBootstrapPhaseReady:
		return "ready"
	case SnapshotWALBootstrapPhaseActive:
		return "active"
	case SnapshotWALBootstrapPhaseAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// SnapshotWALBootstrapOptions configures one join coordinator. The coordinator
// stores no journal records; MaxWALGap only prevents accepting a stale snapshot
// whose replay interval is clearly outside the operator's recovery envelope.
type SnapshotWALBootstrapOptions struct {
	MaxWALGap uint64
}

// SnapshotWALBootstrapPlan identifies the immutable snapshot and the exact WAL
// boundary a joiner must reach before activation.
type SnapshotWALBootstrapPlan struct {
	JoinerID                string
	SourceID                string
	SnapshotID              string
	StorageGeneration       uint64
	SnapshotJournalSequence uint64
	TargetJournalSequence   uint64
	FencingToken            uint64
}

// SnapshotWALBootstrapState is an independent status snapshot. Generation
// changes on every successful lifecycle or progress transition, allowing stale
// operators to be rejected without retaining mutable handles.
type SnapshotWALBootstrapState struct {
	Phase                  SnapshotWALBootstrapPhase
	Plan                   SnapshotWALBootstrapPlan
	Generation             uint64
	AppliedJournalSequence uint64
	AbortReason            string
}

// SnapshotWALBootstrapCoordinator validates and tracks one snapshot-plus-WAL
// join. It does not move files, read a journal, elect a leader, or publish
// traffic. Those side effects remain with the embedding replication control
// plane, which must call the methods only after each side effect succeeds.
type SnapshotWALBootstrapCoordinator struct {
	mu        sync.RWMutex
	maxWALGap uint64
	state     SnapshotWALBootstrapState
}

// NewSnapshotWALBootstrapCoordinator creates a coordinator with bounded replay
// admission. A zero MaxWALGap selects DefaultSnapshotWALBootstrapMaxGap.
func NewSnapshotWALBootstrapCoordinator(options SnapshotWALBootstrapOptions) (*SnapshotWALBootstrapCoordinator, error) {
	maxWALGap := options.MaxWALGap
	if maxWALGap == 0 {
		maxWALGap = DefaultSnapshotWALBootstrapMaxGap
	}
	if maxWALGap > MaxSnapshotWALBootstrapMaxGap {
		return nil, fmt.Errorf("%w: max WAL gap %d exceeds %d", ErrSnapshotWALBootstrapInvalid, maxWALGap, MaxSnapshotWALBootstrapMaxGap)
	}
	return &SnapshotWALBootstrapCoordinator{
		maxWALGap: maxWALGap,
		state:     SnapshotWALBootstrapState{Phase: SnapshotWALBootstrapPhaseIdle},
	}, nil
}

// Begin starts a single join plan. A coordinator is intentionally single-use;
// create a new coordinator for a new snapshot rather than reusing a completed
// or aborted lifecycle.
func (coordinator *SnapshotWALBootstrapCoordinator) Begin(plan SnapshotWALBootstrapPlan) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseIdle {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapAlreadyStarted
	}
	normalized, err := normalizeSnapshotWALBootstrapPlan(plan, coordinator.maxWALGap)
	if err != nil {
		return SnapshotWALBootstrapState{}, err
	}
	coordinator.state = SnapshotWALBootstrapState{
		Phase:      SnapshotWALBootstrapPhaseSnapshotPending,
		Plan:       normalized,
		Generation: 1,
	}
	return coordinator.state, nil
}

// InstallSnapshot records that the caller has installed exactly the planned
// immutable snapshot and is ready to replay journal records after its boundary.
func (coordinator *SnapshotWALBootstrapCoordinator) InstallSnapshot(snapshotID string, storageGeneration, snapshotJournalSequence, fencingToken uint64) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseSnapshotPending {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapFencing
	}
	if snapshotID != coordinator.state.Plan.SnapshotID || storageGeneration != coordinator.state.Plan.StorageGeneration || snapshotJournalSequence != coordinator.state.Plan.SnapshotJournalSequence {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapInvalid
	}
	coordinator.state.Phase = SnapshotWALBootstrapPhaseCatchingUp
	coordinator.state.AppliedJournalSequence = snapshotJournalSequence
	coordinator.state.Generation++
	return coordinator.state, nil
}

// AdvanceWAL records that the caller has successfully and contiguously applied
// all journal records through appliedThrough. Repeating the same value is
// idempotent; the coordinator never accepts progress beyond the target.
func (coordinator *SnapshotWALBootstrapCoordinator) AdvanceWAL(appliedThrough, fencingToken uint64) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseCatchingUp {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapFencing
	}
	if appliedThrough < coordinator.state.AppliedJournalSequence || appliedThrough > coordinator.state.Plan.TargetJournalSequence {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapSequence
	}
	if appliedThrough != coordinator.state.AppliedJournalSequence {
		coordinator.state.AppliedJournalSequence = appliedThrough
		coordinator.state.Generation++
	}
	return coordinator.state, nil
}

// MarkReady fences the final readiness observation to the current generation.
// The caller should perform health checks and any local integrity checks before
// invoking it.
func (coordinator *SnapshotWALBootstrapCoordinator) MarkReady(expectedGeneration, fencingToken uint64) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseCatchingUp {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapFencing
	}
	if expectedGeneration != coordinator.state.Generation {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapGeneration
	}
	if coordinator.state.AppliedJournalSequence != coordinator.state.Plan.TargetJournalSequence {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNotReady
	}
	coordinator.state.Phase = SnapshotWALBootstrapPhaseReady
	coordinator.state.Generation++
	return coordinator.state, nil
}

// Activate commits the coordinator's local activation state after the caller
// has verified the exact current generation and fencing token. The caller owns
// the external atomic publication of the joiner in its serving topology.
func (coordinator *SnapshotWALBootstrapCoordinator) Activate(expectedGeneration, fencingToken uint64) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseReady {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapFencing
	}
	if expectedGeneration != coordinator.state.Generation {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapGeneration
	}
	coordinator.state.Phase = SnapshotWALBootstrapPhaseActive
	coordinator.state.Generation++
	return coordinator.state, nil
}

// Abort stops a non-active join and records a bounded operator reason. An
// active or already aborted coordinator cannot be moved backward.
func (coordinator *SnapshotWALBootstrapCoordinator) Abort(reason string, expectedGeneration, fencingToken uint64) (SnapshotWALBootstrapState, error) {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SnapshotWALBootstrapPhaseSnapshotPending && coordinator.state.Phase != SnapshotWALBootstrapPhaseCatchingUp && coordinator.state.Phase != SnapshotWALBootstrapPhaseReady {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapFencing
	}
	if expectedGeneration != coordinator.state.Generation {
		return SnapshotWALBootstrapState{}, ErrSnapshotWALBootstrapGeneration
	}
	reason, err := normalizeSnapshotWALBootstrapReason(reason)
	if err != nil {
		return SnapshotWALBootstrapState{}, err
	}
	coordinator.state.Phase = SnapshotWALBootstrapPhaseAborted
	coordinator.state.AbortReason = reason
	coordinator.state.Generation++
	return coordinator.state, nil
}

// Snapshot returns a consistent independent status copy. A nil coordinator
// returns the zero idle state.
func (coordinator *SnapshotWALBootstrapCoordinator) Snapshot() SnapshotWALBootstrapState {
	if coordinator == nil {
		return SnapshotWALBootstrapState{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.state
}

func normalizeSnapshotWALBootstrapPlan(plan SnapshotWALBootstrapPlan, maxWALGap uint64) (SnapshotWALBootstrapPlan, error) {
	var err error
	if plan.JoinerID, err = normalizeSnapshotWALBootstrapIdentifier(plan.JoinerID, "joiner ID"); err != nil {
		return SnapshotWALBootstrapPlan{}, err
	}
	if plan.SourceID, err = normalizeSnapshotWALBootstrapIdentifier(plan.SourceID, "source ID"); err != nil {
		return SnapshotWALBootstrapPlan{}, err
	}
	if plan.SnapshotID, err = normalizeSnapshotWALBootstrapIdentifier(plan.SnapshotID, "snapshot ID"); err != nil {
		return SnapshotWALBootstrapPlan{}, err
	}
	if plan.StorageGeneration == 0 {
		return SnapshotWALBootstrapPlan{}, fmt.Errorf("%w: storage generation is required", ErrSnapshotWALBootstrapInvalid)
	}
	if plan.TargetJournalSequence < plan.SnapshotJournalSequence {
		return SnapshotWALBootstrapPlan{}, fmt.Errorf("%w: target journal sequence precedes snapshot", ErrSnapshotWALBootstrapInvalid)
	}
	if plan.TargetJournalSequence-plan.SnapshotJournalSequence > maxWALGap {
		return SnapshotWALBootstrapPlan{}, ErrSnapshotWALBootstrapLimit
	}
	if plan.FencingToken == 0 {
		return SnapshotWALBootstrapPlan{}, fmt.Errorf("%w: non-zero fencing token is required", ErrSnapshotWALBootstrapInvalid)
	}
	return plan, nil
}

func normalizeSnapshotWALBootstrapIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxSnapshotWALBootstrapIdentifierBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: invalid %s", ErrSnapshotWALBootstrapInvalid, label)
	}
	return value, nil
}

func normalizeSnapshotWALBootstrapReason(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxSnapshotWALBootstrapReasonBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: invalid abort reason", ErrSnapshotWALBootstrapInvalid)
	}
	return value, nil
}
