package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const maxSupervisedFailoverIdentityBytes = 256

var (
	// ErrSupervisedFailoverNil indicates a method call on a nil supervisor.
	ErrSupervisedFailoverNil = errors.New("hatReplication: supervised failover coordinator is nil")
	// ErrSupervisedFailoverInvalid identifies malformed decisions, operators,
	// reports, or configuration.
	ErrSupervisedFailoverInvalid = errors.New("hatReplication: supervised failover input is invalid")
	// ErrSupervisedFailoverDisabled indicates that supervision is not enabled.
	ErrSupervisedFailoverDisabled = errors.New("hatReplication: supervised failover is disabled")
	// ErrSupervisedFailoverPhase identifies an operation that is not valid in
	// the current lifecycle phase.
	ErrSupervisedFailoverPhase = errors.New("hatReplication: supervised failover phase does not permit the operation")
	// ErrSupervisedFailoverGeneration identifies a stale lifecycle generation.
	ErrSupervisedFailoverGeneration = errors.New("hatReplication: supervised failover generation mismatch")
	// ErrSupervisedFailoverFencing identifies a changed proposal or fence.
	ErrSupervisedFailoverFencing = errors.New("hatReplication: supervised failover proposal or fence mismatch")
	// ErrSupervisedFailoverOperator identifies a missing or malformed operator
	// identity.
	ErrSupervisedFailoverOperator = errors.New("hatReplication: supervised failover operator is invalid")
	// ErrSupervisedFailoverApprovalRequired prevents an unapproved proposal
	// from committing.
	ErrSupervisedFailoverApprovalRequired = errors.New("hatReplication: supervised failover requires operator approval")
	// ErrSupervisedFailoverRecoveryNotReady indicates that recovery has not
	// reached a healthy, caught-up state.
	ErrSupervisedFailoverRecoveryNotReady = errors.New("hatReplication: supervised failover recovery is not ready")
)

// SupervisedFailoverOptions configures the opt-in lifecycle coordinator.
// Disabled options preserve the existing automatic/manual failover behavior.
type SupervisedFailoverOptions struct {
	Enabled bool
}

// SupervisedFailoverPhase identifies the operator-visible lifecycle.
type SupervisedFailoverPhase uint8

const (
	SupervisedFailoverPhaseIdle SupervisedFailoverPhase = iota
	SupervisedFailoverPhaseProposed
	SupervisedFailoverPhaseApproved
	SupervisedFailoverPhaseCommitted
	SupervisedFailoverPhaseRecovering
	SupervisedFailoverPhaseRecovered
	SupervisedFailoverPhaseRecoveryFailed
	SupervisedFailoverPhaseCancelled
)

// String returns a stable phase name for status and monitoring output.
func (phase SupervisedFailoverPhase) String() string {
	switch phase {
	case SupervisedFailoverPhaseIdle:
		return "idle"
	case SupervisedFailoverPhaseProposed:
		return "proposed"
	case SupervisedFailoverPhaseApproved:
		return "approved"
	case SupervisedFailoverPhaseCommitted:
		return "committed"
	case SupervisedFailoverPhaseRecovering:
		return "recovering"
	case SupervisedFailoverPhaseRecovered:
		return "recovered"
	case SupervisedFailoverPhaseRecoveryFailed:
		return "recovery_failed"
	case SupervisedFailoverPhaseCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// SupervisedFailoverRecoveryPhase identifies recovery progress after commit.
type SupervisedFailoverRecoveryPhase uint8

const (
	SupervisedFailoverRecoveryNone SupervisedFailoverRecoveryPhase = iota
	SupervisedFailoverRecoveryInProgress
	SupervisedFailoverRecoverySucceeded
	SupervisedFailoverRecoveryFailed
)

// String returns a stable recovery phase name.
func (phase SupervisedFailoverRecoveryPhase) String() string {
	switch phase {
	case SupervisedFailoverRecoveryNone:
		return "none"
	case SupervisedFailoverRecoveryInProgress:
		return "in_progress"
	case SupervisedFailoverRecoverySucceeded:
		return "succeeded"
	case SupervisedFailoverRecoveryFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// SupervisedFailoverProposal binds an automatic failover decision to one
// operator-visible lifecycle generation.
type SupervisedFailoverProposal struct {
	Generation uint64
	Decision   AutomaticFailoverDecision
}

// SupervisedFailoverRecoveryReport is supplied by the recovery controller.
// Recovery succeeds only after the promoted candidate is healthy, has the
// committed fencing token, and has applied through the failed source sequence.
type SupervisedFailoverRecoveryReport struct {
	NodeID          string
	Healthy         bool
	AppliedSequence uint64
	FencingToken    uint64
}

// SupervisedFailoverRecoveryState is a detached recovery status view.
type SupervisedFailoverRecoveryState struct {
	Phase           SupervisedFailoverRecoveryPhase
	OperatorID      string
	AppliedSequence uint64
	FencingToken    uint64
	Reason          string
}

// SupervisedFailoverState is a detached lifecycle snapshot. Proposal remains
// available after commit or failure for audit and exact retry fencing.
type SupervisedFailoverState struct {
	Phase       SupervisedFailoverPhase
	Generation  uint64
	HasProposal bool
	Proposal    SupervisedFailoverProposal
	OperatorID  string
	Override    bool
	Reason      string
	Recovery    SupervisedFailoverRecoveryState
}

// SupervisedFailoverCoordinator adds explicit human approval and recovery
// tracking around an AutomaticFailoverDecision. It has no network, storage,
// timer, or automatic commit side effect; callers own those integrations.
type SupervisedFailoverCoordinator struct {
	mu      sync.RWMutex
	enabled bool
	state   SupervisedFailoverState
}

// NewSupervisedFailoverCoordinator creates a default-off lifecycle
// coordinator. The first proposal starts at generation one.
func NewSupervisedFailoverCoordinator(options SupervisedFailoverOptions) (*SupervisedFailoverCoordinator, error) {
	return &SupervisedFailoverCoordinator{
		enabled: options.Enabled,
		state: SupervisedFailoverState{
			Phase:      SupervisedFailoverPhaseIdle,
			Generation: 1,
		},
	}, nil
}

// Enabled reports whether the coordinator accepts lifecycle transitions.
func (coordinator *SupervisedFailoverCoordinator) Enabled() bool {
	return coordinator != nil && coordinator.enabled
}

// Propose records one validated automatic decision and waits for an operator
// approval or override before commit.
func (coordinator *SupervisedFailoverCoordinator) Propose(decision AutomaticFailoverDecision) (SupervisedFailoverProposal, error) {
	if coordinator == nil {
		return SupervisedFailoverProposal{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverProposal{}, ErrSupervisedFailoverDisabled
	}
	normalized, err := normalizeSupervisedFailoverDecision(decision)
	if err != nil {
		return SupervisedFailoverProposal{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseIdle {
		return SupervisedFailoverProposal{}, ErrSupervisedFailoverPhase
	}
	proposal := SupervisedFailoverProposal{Generation: coordinator.state.Generation, Decision: normalized}
	coordinator.state = SupervisedFailoverState{
		Phase:       SupervisedFailoverPhaseProposed,
		Generation:  coordinator.state.Generation,
		HasProposal: true,
		Proposal:    proposal,
	}
	return proposal, nil
}

// Approve records a named operator approval. Approval does not itself commit
// the external topology; callers still call Commit with the exact proposal.
func (coordinator *SupervisedFailoverCoordinator) Approve(expectedGeneration uint64, operatorID string) (SupervisedFailoverState, error) {
	return coordinator.operatorDecision(expectedGeneration, operatorID, false)
}

// Override records an explicit named operator override. It bypasses no
// proposal or fencing check; it only makes the approval source auditable.
func (coordinator *SupervisedFailoverCoordinator) Override(expectedGeneration uint64, operatorID string) (SupervisedFailoverState, error) {
	return coordinator.operatorDecision(expectedGeneration, operatorID, true)
}

// Reject cancels a pending proposal with a required operator reason.
func (coordinator *SupervisedFailoverCoordinator) Reject(expectedGeneration uint64, operatorID, reason string) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	operatorID, err := normalizeSupervisedFailoverOperator(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	reason, err = normalizeSupervisedFailoverReason(reason)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseProposed || !coordinator.state.HasProposal {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	if err := incrementSupervisedFailoverGeneration(&coordinator.state.Generation); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseCancelled
	coordinator.state.OperatorID = operatorID
	coordinator.state.Override = false
	coordinator.state.Reason = reason
	return coordinator.state, nil
}

// Commit accepts exactly the approved proposal and advances the lifecycle
// generation. It does not perform the external topology change.
func (coordinator *SupervisedFailoverCoordinator) Commit(proposal SupervisedFailoverProposal) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseApproved || !coordinator.state.HasProposal {
		if coordinator.state.Phase == SupervisedFailoverPhaseProposed {
			return SupervisedFailoverState{}, ErrSupervisedFailoverApprovalRequired
		}
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if proposal.Generation != coordinator.state.Proposal.Generation || proposal.Generation != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	if proposal.Decision != coordinator.state.Proposal.Decision {
		return SupervisedFailoverState{}, ErrSupervisedFailoverFencing
	}
	if err := incrementSupervisedFailoverGeneration(&coordinator.state.Generation); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseCommitted
	coordinator.state.Recovery = SupervisedFailoverRecoveryState{}
	coordinator.state.Reason = ""
	return coordinator.state, nil
}

// BeginRecovery records that an operator has handed the committed promotion
// to the recovery controller.
func (coordinator *SupervisedFailoverCoordinator) BeginRecovery(expectedGeneration uint64, operatorID string) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	operatorID, err := normalizeSupervisedFailoverOperator(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseCommitted {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	coordinator.state.Phase = SupervisedFailoverPhaseRecovering
	coordinator.state.Recovery = SupervisedFailoverRecoveryState{
		Phase:      SupervisedFailoverRecoveryInProgress,
		OperatorID: operatorID,
	}
	return coordinator.state, nil
}

// CompleteRecovery marks the candidate recovered only after it is healthy,
// uses the committed fencing token, and has caught up through the old source.
func (coordinator *SupervisedFailoverCoordinator) CompleteRecovery(expectedGeneration uint64, report SupervisedFailoverRecoveryReport) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseRecovering {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	expectedToken := coordinator.state.Proposal.Decision.FencingToken
	nodeID, err := normalizeSupervisedFailoverIdentity(report.NodeID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	if nodeID != coordinator.state.Proposal.Decision.CandidateID || report.FencingToken != expectedToken {
		return SupervisedFailoverState{}, ErrSupervisedFailoverFencing
	}
	if !report.Healthy || report.AppliedSequence < coordinator.state.Proposal.Decision.SourceSequence {
		return SupervisedFailoverState{}, ErrSupervisedFailoverRecoveryNotReady
	}
	if err := incrementSupervisedFailoverGeneration(&coordinator.state.Generation); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseRecovered
	coordinator.state.Recovery.Phase = SupervisedFailoverRecoverySucceeded
	coordinator.state.Recovery.AppliedSequence = report.AppliedSequence
	coordinator.state.Recovery.FencingToken = report.FencingToken
	coordinator.state.Reason = ""
	return coordinator.state, nil
}

// FailRecovery records an operator-confirmed recovery failure and leaves the
// lifecycle terminal until Reset is explicitly called.
func (coordinator *SupervisedFailoverCoordinator) FailRecovery(expectedGeneration uint64, operatorID, reason string) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	operatorID, err := normalizeSupervisedFailoverOperator(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	reason, err = normalizeSupervisedFailoverReason(reason)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseRecovering {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	if err := incrementSupervisedFailoverGeneration(&coordinator.state.Generation); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseRecoveryFailed
	coordinator.state.Recovery.Phase = SupervisedFailoverRecoveryFailed
	coordinator.state.Recovery.OperatorID = operatorID
	coordinator.state.Reason = reason
	return coordinator.state, nil
}

// Reset clears a terminal lifecycle only with an explicit operator action.
func (coordinator *SupervisedFailoverCoordinator) Reset(expectedGeneration uint64, operatorID string) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	operatorID, err := normalizeSupervisedFailoverOperator(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseRecovered && coordinator.state.Phase != SupervisedFailoverPhaseRecoveryFailed && coordinator.state.Phase != SupervisedFailoverPhaseCancelled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	if err := incrementSupervisedFailoverGeneration(&coordinator.state.Generation); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state = SupervisedFailoverState{
		Phase:      SupervisedFailoverPhaseIdle,
		Generation: coordinator.state.Generation,
	}
	return coordinator.state, nil
}

// Snapshot returns a detached lifecycle view.
func (coordinator *SupervisedFailoverCoordinator) Snapshot() SupervisedFailoverState {
	if coordinator == nil {
		return SupervisedFailoverState{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return coordinator.state
}

func (coordinator *SupervisedFailoverCoordinator) operatorDecision(expectedGeneration uint64, operatorID string, override bool) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	if !coordinator.enabled {
		return SupervisedFailoverState{}, ErrSupervisedFailoverDisabled
	}
	operatorID, err := normalizeSupervisedFailoverOperator(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseProposed || !coordinator.state.HasProposal {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return SupervisedFailoverState{}, ErrSupervisedFailoverGeneration
	}
	coordinator.state.Phase = SupervisedFailoverPhaseApproved
	coordinator.state.OperatorID = operatorID
	coordinator.state.Override = override
	return coordinator.state, nil
}

func normalizeSupervisedFailoverDecision(decision AutomaticFailoverDecision) (AutomaticFailoverDecision, error) {
	sourceID, err := normalizeSupervisedFailoverIdentity(decision.SourceID)
	if err != nil {
		return AutomaticFailoverDecision{}, err
	}
	candidateID, err := normalizeSupervisedFailoverIdentity(decision.CandidateID)
	if err != nil {
		return AutomaticFailoverDecision{}, err
	}
	if sourceID == "" || candidateID == "" || sourceID == candidateID {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: source and candidate identities are invalid", ErrSupervisedFailoverInvalid)
	}
	if decision.FencingToken == 0 || decision.FencingToken == ^uint64(0) || decision.TopologyGeneration == 0 || decision.TopologyGeneration == ^uint64(0) {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: fencing or topology generation is invalid", ErrSupervisedFailoverInvalid)
	}
	if decision.QuorumSize < 1 || decision.HealthyVoterCount < decision.QuorumSize {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: quorum is invalid", ErrSupervisedFailoverInvalid)
	}
	decision.SourceID = sourceID
	decision.CandidateID = candidateID
	return decision, nil
}

func normalizeSupervisedFailoverIdentity(identity string) (string, error) {
	identity = strings.TrimSpace(identity)
	if identity == "" || len(identity) > maxSupervisedFailoverIdentityBytes || strings.IndexByte(identity, 0) >= 0 {
		return "", fmt.Errorf("%w: identity is invalid", ErrSupervisedFailoverInvalid)
	}
	return identity, nil
}

func normalizeSupervisedFailoverOperator(operatorID string) (string, error) {
	operatorID = strings.TrimSpace(operatorID)
	if operatorID == "" || len(operatorID) > maxSupervisedFailoverIdentityBytes || strings.IndexByte(operatorID, 0) >= 0 {
		return "", ErrSupervisedFailoverOperator
	}
	return operatorID, nil
}

func normalizeSupervisedFailoverReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > maxSupervisedFailoverIdentityBytes || strings.IndexByte(reason, 0) >= 0 {
		return "", fmt.Errorf("%w: reason is invalid", ErrSupervisedFailoverInvalid)
	}
	return reason, nil
}

func incrementSupervisedFailoverGeneration(generation *uint64) error {
	if *generation == ^uint64(0) {
		return ErrSupervisedFailoverGeneration
	}
	*generation++
	return nil
}
