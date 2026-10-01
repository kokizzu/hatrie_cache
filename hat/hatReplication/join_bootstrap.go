package hatReplication

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// JoinBootstrapVersion is the JSON-compatible state format version.
	JoinBootstrapVersion uint8 = 1
	// MaxJoinBootstrapIdentityBytes bounds operation and node identities.
	MaxJoinBootstrapIdentityBytes = 256
	// MaxJoinBootstrapAbortReasonBytes bounds persisted failure detail.
	MaxJoinBootstrapAbortReasonBytes = 512
)

var (
	// ErrJoinBootstrapInvalid indicates malformed or incomplete state.
	ErrJoinBootstrapInvalid = errors.New("hatriecache: join bootstrap state is invalid")
	// ErrJoinBootstrapPhase indicates an invalid state transition.
	ErrJoinBootstrapPhase = errors.New("hatriecache: join bootstrap phase transition is invalid")
	// ErrJoinBootstrapStaleFence indicates an activation attempt from an old
	// topology generation.
	ErrJoinBootstrapStaleFence = errors.New("hatriecache: join bootstrap fencing token is stale")
	// ErrJoinBootstrapNotCaughtUp indicates that the target has not applied the
	// complete snapshot-plus-WAL fence requested for activation.
	ErrJoinBootstrapNotCaughtUp = errors.New("hatriecache: join bootstrap target is not caught up")
	// ErrJoinBootstrapProgressRegressed indicates duplicate-safe progress moved
	// backwards.
	ErrJoinBootstrapProgressRegressed = errors.New("hatriecache: join bootstrap progress regressed")
)

// JoinBootstrapPhase is the durable snapshot-plus-WAL join lifecycle.
type JoinBootstrapPhase uint8

const (
	JoinBootstrapPlanned JoinBootstrapPhase = iota + 1
	JoinBootstrapCatchingUp
	JoinBootstrapReady
	JoinBootstrapActivated
	JoinBootstrapAborted
)

// String returns the stable wire spelling for a join phase.
func (phase JoinBootstrapPhase) String() string {
	switch phase {
	case JoinBootstrapPlanned:
		return "planned"
	case JoinBootstrapCatchingUp:
		return "catching_up"
	case JoinBootstrapReady:
		return "ready"
	case JoinBootstrapActivated:
		return "activated"
	case JoinBootstrapAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// JoinBootstrapState is a compact, copyable coordinator state. Callers can
// persist this value as JSON between process attempts; every transition
// validates the complete state before returning a new value.
type JoinBootstrapState struct {
	Version          uint8              `json:"version"`
	OperationID      string             `json:"operation_id"`
	SourceID         string             `json:"source_id"`
	NodeID           string             `json:"node_id"`
	FencingToken     uint64             `json:"fencing_token"`
	SnapshotSequence uint64             `json:"snapshot_sequence"`
	AppliedSequence  uint64             `json:"applied_sequence"`
	SourceSequence   uint64             `json:"source_sequence,omitempty"`
	Phase            JoinBootstrapPhase `json:"phase"`
	AbortReason      string             `json:"abort_reason,omitempty"`
}

// NewJoinBootstrapState creates a planned join bound to one source, target,
// and topology fencing token. SnapshotSequence is the journal coordinate
// contained in the installed snapshot; applied progress starts there.
func NewJoinBootstrapState(operationID, sourceID, nodeID string, fencingToken, snapshotSequence uint64) (JoinBootstrapState, error) {
	var state JoinBootstrapState
	var err error
	if state.OperationID, err = normalizeJoinBootstrapIdentity(operationID); err != nil {
		return JoinBootstrapState{}, err
	}
	if state.SourceID, err = normalizeJoinBootstrapIdentity(sourceID); err != nil {
		return JoinBootstrapState{}, err
	}
	if state.NodeID, err = normalizeJoinBootstrapIdentity(nodeID); err != nil {
		return JoinBootstrapState{}, err
	}
	if fencingToken == 0 {
		return JoinBootstrapState{}, fmt.Errorf("%w: fencing token must be positive", ErrJoinBootstrapInvalid)
	}
	state.Version = JoinBootstrapVersion
	state.FencingToken = fencingToken
	state.SnapshotSequence = snapshotSequence
	state.AppliedSequence = snapshotSequence
	state.Phase = JoinBootstrapPlanned
	return state, nil
}

// Validate checks a state loaded from durable storage before it is resumed.
func (state JoinBootstrapState) Validate() error {
	if state.Version != JoinBootstrapVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrJoinBootstrapInvalid, state.Version)
	}
	if _, err := normalizeJoinBootstrapIdentity(state.OperationID); err != nil {
		return err
	}
	if _, err := normalizeJoinBootstrapIdentity(state.SourceID); err != nil {
		return err
	}
	if _, err := normalizeJoinBootstrapIdentity(state.NodeID); err != nil {
		return err
	}
	if state.FencingToken == 0 {
		return fmt.Errorf("%w: fencing token must be positive", ErrJoinBootstrapInvalid)
	}
	if state.AppliedSequence < state.SnapshotSequence {
		return fmt.Errorf("%w: applied sequence %d is before snapshot sequence %d", ErrJoinBootstrapInvalid, state.AppliedSequence, state.SnapshotSequence)
	}
	if len(state.AbortReason) > MaxJoinBootstrapAbortReasonBytes || strings.TrimSpace(state.AbortReason) != state.AbortReason {
		return fmt.Errorf("%w: abort reason is invalid", ErrJoinBootstrapInvalid)
	}
	switch state.Phase {
	case JoinBootstrapPlanned:
		if state.AppliedSequence != state.SnapshotSequence || state.SourceSequence != 0 || state.AbortReason != "" {
			return fmt.Errorf("%w: planned state has progress or terminal fields", ErrJoinBootstrapInvalid)
		}
	case JoinBootstrapCatchingUp:
		if state.SourceSequence != 0 || state.AbortReason != "" {
			return fmt.Errorf("%w: catching-up state has activation or abort fields", ErrJoinBootstrapInvalid)
		}
	case JoinBootstrapReady, JoinBootstrapActivated:
		if state.SourceSequence < state.SnapshotSequence || state.AppliedSequence < state.SourceSequence || state.AbortReason != "" {
			return fmt.Errorf("%w: activation state has incomplete progress", ErrJoinBootstrapInvalid)
		}
	case JoinBootstrapAborted:
		if state.AbortReason == "" || state.SourceSequence != 0 {
			return fmt.Errorf("%w: aborted state requires only a reason", ErrJoinBootstrapInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown phase %d", ErrJoinBootstrapInvalid, state.Phase)
	}
	return nil
}

// BeginCatchUp moves a planned join into WAL catch-up. Repeating the call on
// an already catching-up state is idempotent.
func (state JoinBootstrapState) BeginCatchUp() (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	switch state.Phase {
	case JoinBootstrapPlanned:
		state.Phase = JoinBootstrapCatchingUp
		return state, nil
	case JoinBootstrapCatchingUp:
		return state, nil
	default:
		return JoinBootstrapState{}, fmt.Errorf("%w: cannot begin catch-up from %s", ErrJoinBootstrapPhase, state.Phase)
	}
}

// RecordApplied records a target journal coordinate. Equal progress is
// idempotent, while lower progress is rejected to prevent duplicate or stale
// apply reports from moving the activation fence backwards.
func (state JoinBootstrapState) RecordApplied(sequence uint64) (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	if state.Phase != JoinBootstrapCatchingUp {
		return JoinBootstrapState{}, fmt.Errorf("%w: cannot record progress from %s", ErrJoinBootstrapPhase, state.Phase)
	}
	if sequence < state.AppliedSequence {
		return JoinBootstrapState{}, fmt.Errorf("%w: current=%d requested=%d", ErrJoinBootstrapProgressRegressed, state.AppliedSequence, sequence)
	}
	state.AppliedSequence = sequence
	return state, nil
}

// PrepareActivation verifies the current topology fence and records the exact
// source sequence that the target has caught up to. Repeating the same prepare
// is idempotent.
func (state JoinBootstrapState) PrepareActivation(fencingToken, sourceSequence uint64) (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	if fencingToken != state.FencingToken {
		return JoinBootstrapState{}, fmt.Errorf("%w: current=%d requested=%d", ErrJoinBootstrapStaleFence, state.FencingToken, fencingToken)
	}
	if state.Phase == JoinBootstrapReady {
		if state.SourceSequence == sourceSequence {
			return state, nil
		}
		return JoinBootstrapState{}, fmt.Errorf("%w: activation sequence already prepared at %d", ErrJoinBootstrapPhase, state.SourceSequence)
	}
	if state.Phase != JoinBootstrapCatchingUp {
		return JoinBootstrapState{}, fmt.Errorf("%w: cannot prepare activation from %s", ErrJoinBootstrapPhase, state.Phase)
	}
	if sourceSequence < state.SnapshotSequence || state.AppliedSequence < sourceSequence {
		return JoinBootstrapState{}, fmt.Errorf("%w: applied=%d required=%d", ErrJoinBootstrapNotCaughtUp, state.AppliedSequence, sourceSequence)
	}
	state.SourceSequence = sourceSequence
	state.Phase = JoinBootstrapReady
	return state, nil
}

// Activate commits membership for the prepared fencing token. Repeating an
// activation with the same token is idempotent; a different token is stale.
func (state JoinBootstrapState) Activate(fencingToken uint64) (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	if fencingToken != state.FencingToken {
		return JoinBootstrapState{}, fmt.Errorf("%w: current=%d requested=%d", ErrJoinBootstrapStaleFence, state.FencingToken, fencingToken)
	}
	if state.Phase == JoinBootstrapActivated {
		return state, nil
	}
	if state.Phase != JoinBootstrapReady {
		return JoinBootstrapState{}, fmt.Errorf("%w: cannot activate from %s", ErrJoinBootstrapPhase, state.Phase)
	}
	state.Phase = JoinBootstrapActivated
	return state, nil
}

// Abort records a failed bootstrap before activation. Repeating the same
// abort is idempotent and leaves the state retryable with Retry.
func (state JoinBootstrapState) Abort(reason string) (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > MaxJoinBootstrapAbortReasonBytes {
		return JoinBootstrapState{}, fmt.Errorf("%w: abort reason is required and bounded", ErrJoinBootstrapInvalid)
	}
	if state.Phase == JoinBootstrapAborted {
		if state.AbortReason == reason {
			return state, nil
		}
		return JoinBootstrapState{}, fmt.Errorf("%w: abort reason already recorded", ErrJoinBootstrapPhase)
	}
	if state.Phase == JoinBootstrapActivated {
		return JoinBootstrapState{}, fmt.Errorf("%w: activated state cannot be aborted", ErrJoinBootstrapPhase)
	}
	state.Phase = JoinBootstrapAborted
	state.SourceSequence = 0
	state.AbortReason = reason
	return state, nil
}

// Retry resumes an aborted bootstrap with the same topology fence and already
// installed snapshot. A changed fence must create a new state instead.
func (state JoinBootstrapState) Retry(fencingToken uint64) (JoinBootstrapState, error) {
	if err := state.Validate(); err != nil {
		return JoinBootstrapState{}, err
	}
	if fencingToken != state.FencingToken {
		return JoinBootstrapState{}, fmt.Errorf("%w: current=%d requested=%d", ErrJoinBootstrapStaleFence, state.FencingToken, fencingToken)
	}
	if state.Phase != JoinBootstrapAborted {
		return JoinBootstrapState{}, fmt.Errorf("%w: cannot retry from %s", ErrJoinBootstrapPhase, state.Phase)
	}
	state.Phase = JoinBootstrapCatchingUp
	state.SourceSequence = 0
	state.AbortReason = ""
	return state, nil
}

func normalizeJoinBootstrapIdentity(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(trimmed) > MaxJoinBootstrapIdentityBytes || value != trimmed {
		return "", fmt.Errorf("%w: identity is empty or exceeds %d bytes", ErrJoinBootstrapInvalid, MaxJoinBootstrapIdentityBytes)
	}
	return trimmed, nil
}
