package hatReplication

import (
	"crypto/hmac"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// MinSupervisedFailoverOverrideTokenBytes prevents accidental weak bearer
	// tokens when the operator path is enabled.
	MinSupervisedFailoverOverrideTokenBytes = 16
	// MaxSupervisedFailoverOverrideTokenBytes bounds copied operator state.
	MaxSupervisedFailoverOverrideTokenBytes = 128
	// MaxSupervisedFailoverOperatorIDBytes bounds one audit identity.
	MaxSupervisedFailoverOperatorIDBytes = 256
	// MaxSupervisedFailoverReasonBytes bounds one operator recovery note.
	MaxSupervisedFailoverReasonBytes = 256
)

var (
	// ErrSupervisedFailoverNil indicates a nil coordinator.
	ErrSupervisedFailoverNil = errors.New("hatReplication: supervised failover coordinator is nil")
	// ErrSupervisedFailoverOptionsInvalid indicates malformed configuration.
	ErrSupervisedFailoverOptionsInvalid = errors.New("hatReplication: supervised failover options are invalid")
	// ErrSupervisedFailoverDisabled indicates the explicit feature flag is off.
	ErrSupervisedFailoverDisabled = errors.New("hatReplication: supervised failover is disabled")
	// ErrSupervisedFailoverAlreadyStarted indicates a second proposal attempt.
	ErrSupervisedFailoverAlreadyStarted = errors.New("hatReplication: supervised failover proposal already started")
	// ErrSupervisedFailoverPhase indicates an invalid lifecycle operation.
	ErrSupervisedFailoverPhase = errors.New("hatReplication: supervised failover phase does not permit the operation")
	// ErrSupervisedFailoverGeneration indicates a stale proposal generation.
	ErrSupervisedFailoverGeneration = errors.New("hatReplication: supervised failover generation mismatch")
	// ErrSupervisedFailoverFencing indicates a changed proposal decision.
	ErrSupervisedFailoverFencing = errors.New("hatReplication: supervised failover proposal fencing mismatch")
	// ErrSupervisedFailoverOverrideDisabled indicates that operator approval is off.
	ErrSupervisedFailoverOverrideDisabled = errors.New("hatReplication: supervised failover operator approval is disabled")
	// ErrSupervisedFailoverOverrideInvalid indicates a bad operator token.
	ErrSupervisedFailoverOverrideInvalid = errors.New("hatReplication: supervised failover operator token is invalid")
	// ErrSupervisedFailoverOperatorInvalid indicates a malformed operator identity.
	ErrSupervisedFailoverOperatorInvalid = errors.New("hatReplication: supervised failover operator identity is invalid")
	// ErrSupervisedFailoverRecoveryInvalid indicates a mismatched recovery result.
	ErrSupervisedFailoverRecoveryInvalid = errors.New("hatReplication: supervised failover recovery result is invalid")
)

// SupervisedFailoverOptions configures the opt-in operator approval path.
// Automatic contains the existing quorum, lag, and candidate policy. Its
// Enabled field is controlled by this outer Enabled flag when a coordinator is
// created, so there is one clear feature switch.
type SupervisedFailoverOptions struct {
	Enabled bool

	Automatic AutomaticFailoverOptions

	AllowOperatorOverride bool
	OperatorOverrideToken []byte
}

// SupervisedFailoverPhase identifies the supervised lifecycle.
type SupervisedFailoverPhase uint8

const (
	SupervisedFailoverPhaseIdle SupervisedFailoverPhase = iota
	SupervisedFailoverPhaseAwaitingApproval
	SupervisedFailoverPhaseApproved
	SupervisedFailoverPhaseRecovering
	SupervisedFailoverPhaseRecovered
	SupervisedFailoverPhaseAborted
)

// String returns a stable phase name for status and monitoring output.
func (phase SupervisedFailoverPhase) String() string {
	switch phase {
	case SupervisedFailoverPhaseIdle:
		return "idle"
	case SupervisedFailoverPhaseAwaitingApproval:
		return "awaiting_approval"
	case SupervisedFailoverPhaseApproved:
		return "approved"
	case SupervisedFailoverPhaseRecovering:
		return "recovering"
	case SupervisedFailoverPhaseRecovered:
		return "recovered"
	case SupervisedFailoverPhaseAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// SupervisedFailoverRecovery is the caller's post-promotion result. The
// candidate, fence, topology generation, and minimum applied sequence remain
// bound to the approved proposal.
type SupervisedFailoverRecovery struct {
	CandidateID        string
	FencingToken       uint64
	TopologyGeneration uint64
	AppliedSequence    uint64
}

// SupervisedFailoverState is a detached lifecycle snapshot. It contains no
// operator token; the identity and reason are audit metadata only.
type SupervisedFailoverState struct {
	Phase       SupervisedFailoverPhase
	Generation  uint64
	HasProposal bool
	Proposal    AutomaticFailoverProposal
	OperatorID  string
	HasRecovery bool
	Recovery    SupervisedFailoverRecovery
	AbortReason string
}

// SupervisedFailoverCoordinator adds an explicit, token-gated operator step
// around the existing automatic failover evaluator. It has no network or
// storage side effects; the embedding service executes promotion and calls
// CompleteRecovery after its own recovery checks.
type SupervisedFailoverCoordinator struct {
	mu                    sync.RWMutex
	enabled               bool
	automatic             *AutomaticFailoverCoordinator
	allowOperatorOverride bool
	operatorOverrideToken []byte
	state                 SupervisedFailoverState
}

// NewSupervisedFailoverCoordinator creates a coordinator. Both Enabled and
// AllowOperatorOverride are false by default.
func NewSupervisedFailoverCoordinator(options SupervisedFailoverOptions) (*SupervisedFailoverCoordinator, error) {
	if err := validateSupervisedFailoverOptions(options); err != nil {
		return nil, err
	}
	automaticOptions := options.Automatic
	automaticOptions.Enabled = options.Enabled
	automatic, err := NewAutomaticFailoverCoordinator(automaticOptions)
	if err != nil {
		return nil, err
	}
	return &SupervisedFailoverCoordinator{
		enabled:               options.Enabled,
		automatic:             automatic,
		allowOperatorOverride: options.AllowOperatorOverride,
		operatorOverrideToken: append([]byte(nil), options.OperatorOverrideToken...),
	}, nil
}

// Propose evaluates one trusted health observation and waits for an operator
// approval. The underlying automatic policy remains the source of truth for
// quorum, lag, candidate selection, and fencing decisions.
func (coordinator *SupervisedFailoverCoordinator) Propose(observation AutomaticFailoverObservation) (AutomaticFailoverProposal, error) {
	if coordinator == nil {
		return AutomaticFailoverProposal{}, ErrSupervisedFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if !coordinator.enabled {
		return AutomaticFailoverProposal{}, ErrSupervisedFailoverDisabled
	}
	if coordinator.state.Phase != SupervisedFailoverPhaseIdle {
		return AutomaticFailoverProposal{}, ErrSupervisedFailoverAlreadyStarted
	}
	proposal, err := coordinator.automatic.Propose(observation)
	if err != nil {
		return AutomaticFailoverProposal{}, err
	}
	coordinator.state = SupervisedFailoverState{
		Phase:       SupervisedFailoverPhaseAwaitingApproval,
		Generation:  proposal.Generation,
		HasProposal: true,
		Proposal:    proposal,
	}
	return proposal, nil
}

// Approve authorizes one exact proposal with the configured operator token.
// Authentication and secure token delivery remain responsibilities of the
// embedding service; this method only performs the local constant-time check.
func (coordinator *SupervisedFailoverCoordinator) Approve(proposal AutomaticFailoverProposal, operatorID string, token []byte) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseAwaitingApproval {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if err := coordinator.checkProposal(proposal); err != nil {
		return SupervisedFailoverState{}, err
	}
	if !coordinator.allowOperatorOverride {
		return SupervisedFailoverState{}, ErrSupervisedFailoverOverrideDisabled
	}
	normalizedOperatorID, err := normalizeSupervisedFailoverOperatorID(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	if !hmac.Equal(token, coordinator.operatorOverrideToken) {
		return SupervisedFailoverState{}, ErrSupervisedFailoverOverrideInvalid
	}
	coordinator.state.Phase = SupervisedFailoverPhaseApproved
	coordinator.state.OperatorID = normalizedOperatorID
	return cloneSupervisedFailoverState(coordinator.state), nil
}

// StartRecovery atomically consumes the approved proposal's local automatic
// fence and enters the recovery phase. The caller then performs the external
// topology promotion and data-recovery work.
func (coordinator *SupervisedFailoverCoordinator) StartRecovery(proposal AutomaticFailoverProposal) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseApproved {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if err := coordinator.checkProposal(proposal); err != nil {
		return SupervisedFailoverState{}, err
	}
	if _, err := coordinator.automatic.Commit(proposal); err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseRecovering
	return cloneSupervisedFailoverState(coordinator.state), nil
}

// CompleteRecovery records a successful recovery only when its identity and
// fencing values still match the approved proposal.
func (coordinator *SupervisedFailoverCoordinator) CompleteRecovery(proposal AutomaticFailoverProposal, recovery SupervisedFailoverRecovery) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseRecovering {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if err := coordinator.checkProposal(proposal); err != nil {
		return SupervisedFailoverState{}, err
	}
	validatedRecovery, err := validateSupervisedFailoverRecovery(proposal.Decision, recovery)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	coordinator.state.Phase = SupervisedFailoverPhaseRecovered
	coordinator.state.HasRecovery = true
	coordinator.state.Recovery = validatedRecovery
	return cloneSupervisedFailoverState(coordinator.state), nil
}

// Abort records an operator-decision or recovery failure and permanently
// fences the current event. Before StartRecovery it also cancels the wrapped
// automatic proposal; during recovery the automatic commit is already final.
func (coordinator *SupervisedFailoverCoordinator) Abort(proposal AutomaticFailoverProposal, operatorID string, token []byte, reason string) (SupervisedFailoverState, error) {
	if coordinator == nil {
		return SupervisedFailoverState{}, ErrSupervisedFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != SupervisedFailoverPhaseAwaitingApproval && coordinator.state.Phase != SupervisedFailoverPhaseApproved && coordinator.state.Phase != SupervisedFailoverPhaseRecovering {
		return SupervisedFailoverState{}, ErrSupervisedFailoverPhase
	}
	if err := coordinator.checkProposal(proposal); err != nil {
		return SupervisedFailoverState{}, err
	}
	if !coordinator.allowOperatorOverride {
		return SupervisedFailoverState{}, ErrSupervisedFailoverOverrideDisabled
	}
	normalizedOperatorID, err := normalizeSupervisedFailoverOperatorID(operatorID)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	if !hmac.Equal(token, coordinator.operatorOverrideToken) {
		return SupervisedFailoverState{}, ErrSupervisedFailoverOverrideInvalid
	}
	normalizedReason, err := normalizeSupervisedFailoverReason(reason)
	if err != nil {
		return SupervisedFailoverState{}, err
	}
	if coordinator.state.Phase != SupervisedFailoverPhaseRecovering {
		if _, err := coordinator.automatic.Cancel(proposal.Generation); err != nil {
			return SupervisedFailoverState{}, err
		}
	}
	coordinator.state.Phase = SupervisedFailoverPhaseAborted
	coordinator.state.OperatorID = normalizedOperatorID
	coordinator.state.AbortReason = normalizedReason
	return cloneSupervisedFailoverState(coordinator.state), nil
}

// Snapshot returns a detached lifecycle view.
func (coordinator *SupervisedFailoverCoordinator) Snapshot() SupervisedFailoverState {
	if coordinator == nil {
		return SupervisedFailoverState{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return cloneSupervisedFailoverState(coordinator.state)
}

func (coordinator *SupervisedFailoverCoordinator) checkProposal(proposal AutomaticFailoverProposal) error {
	if !coordinator.state.HasProposal {
		return ErrSupervisedFailoverPhase
	}
	if proposal.Generation != coordinator.state.Generation {
		return ErrSupervisedFailoverGeneration
	}
	if proposal.Decision != coordinator.state.Proposal.Decision {
		return ErrSupervisedFailoverFencing
	}
	return nil
}

func validateSupervisedFailoverOptions(options SupervisedFailoverOptions) error {
	if options.AllowOperatorOverride {
		if len(options.OperatorOverrideToken) < MinSupervisedFailoverOverrideTokenBytes || len(options.OperatorOverrideToken) > MaxSupervisedFailoverOverrideTokenBytes {
			return fmt.Errorf("%w: operator token must be between %d and %d bytes", ErrSupervisedFailoverOptionsInvalid, MinSupervisedFailoverOverrideTokenBytes, MaxSupervisedFailoverOverrideTokenBytes)
		}
	} else if len(options.OperatorOverrideToken) != 0 {
		return fmt.Errorf("%w: operator token requires AllowOperatorOverride", ErrSupervisedFailoverOptionsInvalid)
	}
	return nil
}

func validateSupervisedFailoverRecovery(decision AutomaticFailoverDecision, recovery SupervisedFailoverRecovery) (SupervisedFailoverRecovery, error) {
	candidateID, err := normalizeAutomaticFailoverNodeID(recovery.CandidateID)
	if err != nil {
		return SupervisedFailoverRecovery{}, fmt.Errorf("%w: candidate ID: %v", ErrSupervisedFailoverRecoveryInvalid, err)
	}
	if candidateID != decision.CandidateID {
		return SupervisedFailoverRecovery{}, fmt.Errorf("%w: candidate does not match proposal", ErrSupervisedFailoverRecoveryInvalid)
	}
	if recovery.FencingToken != decision.FencingToken || recovery.TopologyGeneration != decision.TopologyGeneration {
		return SupervisedFailoverRecovery{}, fmt.Errorf("%w: fencing values do not match proposal", ErrSupervisedFailoverRecoveryInvalid)
	}
	if recovery.AppliedSequence < decision.CandidateSequence {
		return SupervisedFailoverRecovery{}, fmt.Errorf("%w: applied sequence regressed", ErrSupervisedFailoverRecoveryInvalid)
	}
	recovery.CandidateID = candidateID
	return recovery, nil
}

func normalizeSupervisedFailoverOperatorID(operatorID string) (string, error) {
	operatorID = strings.TrimSpace(operatorID)
	if operatorID == "" || len(operatorID) > MaxSupervisedFailoverOperatorIDBytes || strings.IndexByte(operatorID, 0) >= 0 {
		return "", fmt.Errorf("%w: operator ID must be non-empty and at most %d bytes", ErrSupervisedFailoverOperatorInvalid, MaxSupervisedFailoverOperatorIDBytes)
	}
	return operatorID, nil
}

func normalizeSupervisedFailoverReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > MaxSupervisedFailoverReasonBytes || strings.IndexByte(reason, 0) >= 0 {
		return "", fmt.Errorf("%w: reason must be non-empty and at most %d bytes", ErrSupervisedFailoverRecoveryInvalid, MaxSupervisedFailoverReasonBytes)
	}
	return reason, nil
}

func cloneSupervisedFailoverState(state SupervisedFailoverState) SupervisedFailoverState {
	return state
}
