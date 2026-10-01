package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrJoinBootstrapInvalid           = errors.New("hatriecache: invalid join bootstrap")
	ErrJoinBootstrapSnapshotRequired  = errors.New("hatriecache: join bootstrap snapshot is required")
	ErrJoinBootstrapSnapshotMismatch  = errors.New("hatriecache: join bootstrap snapshot sequence mismatch")
	ErrJoinBootstrapCatchUpRequired   = errors.New("hatriecache: join bootstrap catch-up is required")
	ErrJoinBootstrapCatchUpBehind     = errors.New("hatriecache: join bootstrap catch-up is behind the snapshot")
	ErrJoinBootstrapCatchUpRegression = errors.New("hatriecache: join bootstrap catch-up regressed")
	ErrJoinBootstrapFenceMismatch     = errors.New("hatriecache: join bootstrap fencing token mismatch")
	ErrJoinBootstrapTerminal          = errors.New("hatriecache: join bootstrap is terminal")
	ErrJoinBootstrapAbortReason       = errors.New("hatriecache: join bootstrap abort reason is required")
)

// JoinBootstrapPhase describes the ordered phases of a replica join.
type JoinBootstrapPhase string

const (
	JoinBootstrapPhasePending           JoinBootstrapPhase = "pending"
	JoinBootstrapPhaseSnapshotInstalled JoinBootstrapPhase = "snapshot_installed"
	JoinBootstrapPhaseCaughtUp          JoinBootstrapPhase = "caught_up"
	JoinBootstrapPhaseActivated         JoinBootstrapPhase = "activated"
	JoinBootstrapPhaseAborted           JoinBootstrapPhase = "aborted"
)

// JoinBootstrapOptions binds a join to one source, one target, an exact
// snapshot journal sequence, and the fencing token that must be used when the
// target becomes active.
type JoinBootstrapOptions struct {
	SourceNode       string
	TargetNode       string
	SnapshotSequence uint64
	FencingToken     uint64
}

// JoinBootstrapState is a copy-safe description of a join's progress.
type JoinBootstrapState struct {
	SourceNode       string
	TargetNode       string
	SnapshotSequence uint64
	AppliedThrough   uint64
	FencingToken     uint64
	Phase            JoinBootstrapPhase
	AbortReason      string
}

// JoinBootstrap enforces the snapshot, ordered journal catch-up, fencing, and
// activation contract for a replica join. It does not own network or storage
// transport; callers perform those operations and report their verified
// results through the transitions below.
type JoinBootstrap struct {
	mu    sync.Mutex
	state JoinBootstrapState
}

// NewJoinBootstrap creates a join bootstrap in the pending phase.
func NewJoinBootstrap(options JoinBootstrapOptions) (*JoinBootstrap, error) {
	source := strings.TrimSpace(options.SourceNode)
	target := strings.TrimSpace(options.TargetNode)
	if source == "" || target == "" {
		return nil, fmt.Errorf("%w: source and target nodes are required", ErrJoinBootstrapInvalid)
	}
	if source == target {
		return nil, fmt.Errorf("%w: source and target nodes must differ", ErrJoinBootstrapInvalid)
	}
	if options.FencingToken == 0 {
		return nil, fmt.Errorf("%w: fencing token must be non-zero", ErrJoinBootstrapInvalid)
	}
	return &JoinBootstrap{state: JoinBootstrapState{
		SourceNode:       source,
		TargetNode:       target,
		SnapshotSequence: options.SnapshotSequence,
		FencingToken:     options.FencingToken,
		Phase:            JoinBootstrapPhasePending,
	}}, nil
}

// Snapshot returns the current join state.
func (bootstrap *JoinBootstrap) Snapshot() JoinBootstrapState {
	if bootstrap == nil {
		return JoinBootstrapState{}
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	return bootstrap.state
}

// SnapshotInstalled records that the target has installed the exact source
// snapshot represented by the configured journal sequence.
func (bootstrap *JoinBootstrap) SnapshotInstalled(sequence uint64) error {
	if bootstrap == nil {
		return ErrJoinBootstrapInvalid
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if bootstrap.state.Phase == JoinBootstrapPhaseAborted || bootstrap.state.Phase == JoinBootstrapPhaseActivated {
		return ErrJoinBootstrapTerminal
	}
	if sequence != bootstrap.state.SnapshotSequence {
		return fmt.Errorf("%w: got %d, want %d", ErrJoinBootstrapSnapshotMismatch, sequence, bootstrap.state.SnapshotSequence)
	}
	if bootstrap.state.Phase == JoinBootstrapPhaseSnapshotInstalled {
		return nil
	}
	if bootstrap.state.Phase != JoinBootstrapPhasePending {
		return fmt.Errorf("%w: cannot install snapshot during %s", ErrJoinBootstrapTerminal, bootstrap.state.Phase)
	}
	bootstrap.state.AppliedThrough = sequence
	bootstrap.state.Phase = JoinBootstrapPhaseSnapshotInstalled
	return nil
}

// CatchUp records a verified journal replay through appliedThrough. Progress
// is monotonic and may be reported repeatedly by retrying callers.
func (bootstrap *JoinBootstrap) CatchUp(appliedThrough uint64) error {
	if bootstrap == nil {
		return ErrJoinBootstrapInvalid
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	switch bootstrap.state.Phase {
	case JoinBootstrapPhasePending:
		return ErrJoinBootstrapSnapshotRequired
	case JoinBootstrapPhaseAborted, JoinBootstrapPhaseActivated:
		return ErrJoinBootstrapTerminal
	}
	if appliedThrough < bootstrap.state.SnapshotSequence {
		return fmt.Errorf("%w: got %d, want at least %d", ErrJoinBootstrapCatchUpBehind, appliedThrough, bootstrap.state.SnapshotSequence)
	}
	if appliedThrough < bootstrap.state.AppliedThrough {
		return fmt.Errorf("%w: got %d, previous %d", ErrJoinBootstrapCatchUpRegression, appliedThrough, bootstrap.state.AppliedThrough)
	}
	bootstrap.state.AppliedThrough = appliedThrough
	bootstrap.state.Phase = JoinBootstrapPhaseCaughtUp
	return nil
}

// Activate atomically marks the target active after catch-up and fencing
// validation. Repeating the exact successful activation is safe.
func (bootstrap *JoinBootstrap) Activate(appliedThrough uint64, fencingToken uint64) (JoinBootstrapState, error) {
	if bootstrap == nil {
		return JoinBootstrapState{}, ErrJoinBootstrapInvalid
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if bootstrap.state.Phase == JoinBootstrapPhaseAborted {
		return bootstrap.state, ErrJoinBootstrapTerminal
	}
	if fencingToken != bootstrap.state.FencingToken {
		return bootstrap.state, fmt.Errorf("%w: got %d, want %d", ErrJoinBootstrapFenceMismatch, fencingToken, bootstrap.state.FencingToken)
	}
	if bootstrap.state.Phase == JoinBootstrapPhaseActivated {
		if appliedThrough == bootstrap.state.AppliedThrough {
			return bootstrap.state, nil
		}
		return bootstrap.state, fmt.Errorf("%w: activation already recorded through %d", ErrJoinBootstrapTerminal, bootstrap.state.AppliedThrough)
	}
	if bootstrap.state.Phase != JoinBootstrapPhaseCaughtUp {
		return bootstrap.state, ErrJoinBootstrapCatchUpRequired
	}
	if appliedThrough < bootstrap.state.AppliedThrough {
		return bootstrap.state, fmt.Errorf("%w: got %d, previous %d", ErrJoinBootstrapCatchUpRegression, appliedThrough, bootstrap.state.AppliedThrough)
	}
	bootstrap.state.AppliedThrough = appliedThrough
	bootstrap.state.Phase = JoinBootstrapPhaseActivated
	return bootstrap.state, nil
}

// Abort makes the join terminal and records why the caller discarded it.
// Repeating the same abort is safe and does not change the original reason.
func (bootstrap *JoinBootstrap) Abort(reason string) error {
	if bootstrap == nil {
		return ErrJoinBootstrapInvalid
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return ErrJoinBootstrapAbortReason
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if bootstrap.state.Phase == JoinBootstrapPhaseAborted {
		if bootstrap.state.AbortReason == reason {
			return nil
		}
		return ErrJoinBootstrapTerminal
	}
	if bootstrap.state.Phase == JoinBootstrapPhaseActivated {
		return ErrJoinBootstrapTerminal
	}
	bootstrap.state.AbortReason = reason
	bootstrap.state.Phase = JoinBootstrapPhaseAborted
	return nil
}
