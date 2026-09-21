package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultHotStandbyMaxLag bounds the unapplied WAL admitted by a standby.
	DefaultHotStandbyMaxLag uint64 = 1 << 20
	// MaxHotStandbyMaxLag prevents an accidentally unbounded replay window.
	MaxHotStandbyMaxLag uint64 = 1 << 32
	// MaxHotStandbyIdentifierBytes bounds node identifiers.
	MaxHotStandbyIdentifierBytes = 256
)

var (
	// ErrHotStandbyNil indicates a nil coordinator.
	ErrHotStandbyNil = errors.New("hatReplication: hot standby coordinator is nil")
	// ErrHotStandbyInvalid indicates malformed options or plan data.
	ErrHotStandbyInvalid = errors.New("hatReplication: hot standby data is invalid")
	// ErrHotStandbyAlreadyStarted indicates that Start was called twice.
	ErrHotStandbyAlreadyStarted = errors.New("hatReplication: hot standby already started")
	// ErrHotStandbyPhase indicates that an operation is not valid in the current phase.
	ErrHotStandbyPhase = errors.New("hatReplication: hot standby phase does not permit the operation")
	// ErrHotStandbyFencing indicates a stale or missing source fence.
	ErrHotStandbyFencing = errors.New("hatReplication: hot standby fencing token mismatch")
	// ErrHotStandbyTerm indicates a stale source or promotion term.
	ErrHotStandbyTerm = errors.New("hatReplication: hot standby term is invalid")
	// ErrHotStandbySequence indicates a WAL sequence regression or gap.
	ErrHotStandbySequence = errors.New("hatReplication: hot standby WAL sequence is invalid")
	// ErrHotStandbyLag indicates that the source head exceeds the configured replay window.
	ErrHotStandbyLag = errors.New("hatReplication: hot standby WAL lag exceeds limit")
	// ErrHotStandbyGeneration indicates a stale lifecycle observation.
	ErrHotStandbyGeneration = errors.New("hatReplication: hot standby generation mismatch")
	// ErrHotStandbyNotCaughtUp indicates that promotion was requested before the source head.
	ErrHotStandbyNotCaughtUp = errors.New("hatReplication: hot standby is not caught up")
)

// HotStandbyPhase identifies the transport-neutral standby lifecycle.
type HotStandbyPhase uint8

const (
	HotStandbyPhaseIdle HotStandbyPhase = iota
	HotStandbyPhaseReplaying
	HotStandbyPhaseCaughtUp
	HotStandbyPhasePrimary
)

// String returns a stable phase name for status and logs.
func (phase HotStandbyPhase) String() string {
	switch phase {
	case HotStandbyPhaseIdle:
		return "idle"
	case HotStandbyPhaseReplaying:
		return "replaying"
	case HotStandbyPhaseCaughtUp:
		return "caught_up"
	case HotStandbyPhasePrimary:
		return "primary"
	default:
		return "unknown"
	}
}

// HotStandbyOptions configures one standby lifecycle.
type HotStandbyOptions struct {
	// MaxLag is the largest permitted difference between the advertised source
	// head and the standby's applied sequence. Zero selects the default.
	MaxLag uint64
}

// HotStandbyPlan identifies the immutable source and installed snapshot.
type HotStandbyPlan struct {
	StandbyID         string
	PrimaryID         string
	StorageGeneration uint64
	SnapshotSequence  uint64
	SourceTerm        uint64
	FencingToken      uint64
}

// HotStandbyState is a detached observation of a standby lifecycle.
type HotStandbyState struct {
	Phase              HotStandbyPhase
	Plan               HotStandbyPlan
	Generation         uint64
	Term               uint64
	AdvertisedSequence uint64
	AppliedSequence    uint64
}

// HotStandbyCoordinator validates continuous replay and local promotion
// readiness. It stores no WAL records and performs no transport, storage, or
// topology side effects; callers apply records and publish a promoted node.
type HotStandbyCoordinator struct {
	mu     sync.RWMutex
	maxLag uint64
	state  HotStandbyState
}

// NewHotStandbyCoordinator creates a bounded standby coordinator. A zero
// MaxLag selects DefaultHotStandbyMaxLag.
func NewHotStandbyCoordinator(options HotStandbyOptions) (*HotStandbyCoordinator, error) {
	maxLag := options.MaxLag
	if maxLag == 0 {
		maxLag = DefaultHotStandbyMaxLag
	}
	if maxLag > MaxHotStandbyMaxLag {
		return nil, fmt.Errorf("%w: max lag %d exceeds %d", ErrHotStandbyInvalid, maxLag, MaxHotStandbyMaxLag)
	}
	return &HotStandbyCoordinator{
		maxLag: maxLag,
		state:  HotStandbyState{Phase: HotStandbyPhaseIdle},
	}, nil
}

// Start begins replay from an already installed snapshot. The caller owns
// snapshot transfer and must use the exact plan values when starting replay.
func (coordinator *HotStandbyCoordinator) Start(plan HotStandbyPlan) (HotStandbyState, error) {
	if coordinator == nil {
		return HotStandbyState{}, ErrHotStandbyNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != HotStandbyPhaseIdle {
		return HotStandbyState{}, ErrHotStandbyAlreadyStarted
	}
	normalized, err := normalizeHotStandbyPlan(plan)
	if err != nil {
		return HotStandbyState{}, err
	}
	coordinator.state = HotStandbyState{
		Phase:              HotStandbyPhaseReplaying,
		Plan:               normalized,
		Generation:         1,
		Term:               normalized.SourceTerm,
		AdvertisedSequence: normalized.SnapshotSequence,
		AppliedSequence:    normalized.SnapshotSequence,
	}
	return coordinator.state, nil
}

// Snapshot returns a detached state observation. A nil coordinator returns the
// zero state.
func (coordinator *HotStandbyCoordinator) Snapshot() HotStandbyState {
	if coordinator == nil {
		return HotStandbyState{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.state
}

// ObserveHead records a monotonic source WAL head. The source head must be
// observed before its records are applied, which prevents a standby from
// silently accepting an unverified future sequence.
func (coordinator *HotStandbyCoordinator) ObserveHead(sourceTerm, advertisedThrough, fencingToken uint64) (HotStandbyState, error) {
	if coordinator == nil {
		return HotStandbyState{}, ErrHotStandbyNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != HotStandbyPhaseReplaying && coordinator.state.Phase != HotStandbyPhaseCaughtUp {
		return HotStandbyState{}, ErrHotStandbyPhase
	}
	if err := coordinator.validateSourceLocked(sourceTerm, fencingToken); err != nil {
		return HotStandbyState{}, err
	}
	if advertisedThrough < coordinator.state.AdvertisedSequence {
		return HotStandbyState{}, ErrHotStandbySequence
	}
	if advertisedThrough-coordinator.state.AppliedSequence > coordinator.maxLag {
		return HotStandbyState{}, ErrHotStandbyLag
	}
	if advertisedThrough == coordinator.state.AdvertisedSequence && coordinator.state.Phase == phaseForHotStandbySequences(coordinator.state.AppliedSequence, advertisedThrough) {
		return coordinator.state, nil
	}
	coordinator.state.AdvertisedSequence = advertisedThrough
	coordinator.state.Phase = phaseForHotStandbySequences(coordinator.state.AppliedSequence, advertisedThrough)
	coordinator.state.Generation++
	return coordinator.state, nil
}

// ApplyWAL records a contiguous, successfully applied WAL batch. Replaying a
// batch whose end is already applied is idempotent; a partial overlap or gap is
// rejected. The caller owns the actual record application.
func (coordinator *HotStandbyCoordinator) ApplyWAL(sourceTerm, firstSequence, appliedThrough, fencingToken uint64) (HotStandbyState, error) {
	if coordinator == nil {
		return HotStandbyState{}, ErrHotStandbyNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != HotStandbyPhaseReplaying && coordinator.state.Phase != HotStandbyPhaseCaughtUp {
		return HotStandbyState{}, ErrHotStandbyPhase
	}
	if err := coordinator.validateSourceLocked(sourceTerm, fencingToken); err != nil {
		return HotStandbyState{}, err
	}
	if firstSequence == 0 || appliedThrough < firstSequence {
		return HotStandbyState{}, ErrHotStandbySequence
	}
	if appliedThrough <= coordinator.state.AppliedSequence {
		if firstSequence > coordinator.state.AppliedSequence {
			return HotStandbyState{}, ErrHotStandbySequence
		}
		return coordinator.state, nil
	}
	if firstSequence != coordinator.state.AppliedSequence+1 || appliedThrough > coordinator.state.AdvertisedSequence {
		return HotStandbyState{}, ErrHotStandbySequence
	}
	coordinator.state.AppliedSequence = appliedThrough
	coordinator.state.Phase = phaseForHotStandbySequences(appliedThrough, coordinator.state.AdvertisedSequence)
	coordinator.state.Generation++
	return coordinator.state, nil
}

// Promote transitions an exactly caught-up standby to primary. The caller
// must fence the old source and atomically publish the new topology around
// this local state transition.
func (coordinator *HotStandbyCoordinator) Promote(expectedGeneration, fencingToken, newTerm uint64) (HotStandbyState, error) {
	if coordinator == nil {
		return HotStandbyState{}, ErrHotStandbyNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase == HotStandbyPhaseReplaying {
		return HotStandbyState{}, ErrHotStandbyNotCaughtUp
	}
	if coordinator.state.Phase != HotStandbyPhaseCaughtUp {
		return HotStandbyState{}, ErrHotStandbyPhase
	}
	if fencingToken != coordinator.state.Plan.FencingToken {
		return HotStandbyState{}, ErrHotStandbyFencing
	}
	if expectedGeneration != coordinator.state.Generation {
		return HotStandbyState{}, ErrHotStandbyGeneration
	}
	if newTerm <= coordinator.state.Term {
		return HotStandbyState{}, ErrHotStandbyTerm
	}
	coordinator.state.Phase = HotStandbyPhasePrimary
	coordinator.state.Term = newTerm
	coordinator.state.Generation++
	return coordinator.state, nil
}

func (coordinator *HotStandbyCoordinator) validateSourceLocked(sourceTerm, fencingToken uint64) error {
	if fencingToken != coordinator.state.Plan.FencingToken {
		return ErrHotStandbyFencing
	}
	if sourceTerm != coordinator.state.Plan.SourceTerm {
		return ErrHotStandbyTerm
	}
	return nil
}

func normalizeHotStandbyPlan(plan HotStandbyPlan) (HotStandbyPlan, error) {
	plan.StandbyID = strings.TrimSpace(plan.StandbyID)
	plan.PrimaryID = strings.TrimSpace(plan.PrimaryID)
	if plan.StandbyID == "" || plan.PrimaryID == "" || plan.StandbyID == plan.PrimaryID {
		return HotStandbyPlan{}, fmt.Errorf("%w: standby and primary identifiers must be distinct", ErrHotStandbyInvalid)
	}
	if len(plan.StandbyID) > MaxHotStandbyIdentifierBytes || len(plan.PrimaryID) > MaxHotStandbyIdentifierBytes {
		return HotStandbyPlan{}, fmt.Errorf("%w: identifier exceeds %d bytes", ErrHotStandbyInvalid, MaxHotStandbyIdentifierBytes)
	}
	if plan.StorageGeneration == 0 || plan.SourceTerm == 0 || plan.FencingToken == 0 {
		return HotStandbyPlan{}, fmt.Errorf("%w: storage generation, source term, and fencing token are required", ErrHotStandbyInvalid)
	}
	return plan, nil
}

func phaseForHotStandbySequences(applied, advertised uint64) HotStandbyPhase {
	if applied == advertised {
		return HotStandbyPhaseCaughtUp
	}
	return HotStandbyPhaseReplaying
}
