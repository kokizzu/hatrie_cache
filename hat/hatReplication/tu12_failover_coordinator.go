package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultAutomaticFailoverMaxCandidates bounds one trusted health
	// observation when MaxCandidates is zero.
	DefaultAutomaticFailoverMaxCandidates = 128
	// MaxAutomaticFailoverMaxCandidates prevents unbounded policy input.
	MaxAutomaticFailoverMaxCandidates = 4096
	// MaxAutomaticFailoverNodeIDBytes bounds one node identity.
	MaxAutomaticFailoverNodeIDBytes = 256
)

var (
	// ErrAutomaticFailoverNil indicates a nil coordinator.
	ErrAutomaticFailoverNil = errors.New("hatReplication: automatic failover coordinator is nil")
	// ErrAutomaticFailoverInvalid indicates malformed policy input.
	ErrAutomaticFailoverInvalid = errors.New("hatReplication: automatic failover input is invalid")
	// ErrAutomaticFailoverDisabled indicates that automatic failover is not
	// enabled for this coordinator.
	ErrAutomaticFailoverDisabled = errors.New("hatReplication: automatic failover is disabled")
	// ErrAutomaticFailoverAlreadyStarted indicates a second proposal attempt.
	ErrAutomaticFailoverAlreadyStarted = errors.New("hatReplication: automatic failover proposal already started")
	// ErrAutomaticFailoverPhase indicates an operation is invalid in the current
	// lifecycle phase.
	ErrAutomaticFailoverPhase = errors.New("hatReplication: automatic failover phase does not permit the operation")
	// ErrAutomaticFailoverSourceHealthy indicates that promotion was requested
	// while the current source is still healthy.
	ErrAutomaticFailoverSourceHealthy = errors.New("hatReplication: automatic failover source is healthy")
	// ErrAutomaticFailoverNoQuorum indicates that the trusted observation does
	// not contain enough healthy voters.
	ErrAutomaticFailoverNoQuorum = errors.New("hatReplication: automatic failover quorum is unavailable")
	// ErrAutomaticFailoverNoCandidate indicates that no healthy, caught-up
	// candidate can be promoted.
	ErrAutomaticFailoverNoCandidate = errors.New("hatReplication: automatic failover has no eligible candidate")
	// ErrAutomaticFailoverGeneration indicates a stale proposal or transition.
	ErrAutomaticFailoverGeneration = errors.New("hatReplication: automatic failover generation mismatch")
	// ErrAutomaticFailoverFencing indicates a proposal was changed after it was
	// created.
	ErrAutomaticFailoverFencing = errors.New("hatReplication: automatic failover proposal fencing mismatch")
)

// AutomaticFailoverPhase identifies the proposal lifecycle.
type AutomaticFailoverPhase uint8

const (
	AutomaticFailoverPhaseIdle AutomaticFailoverPhase = iota
	AutomaticFailoverPhaseProposed
	AutomaticFailoverPhaseCommitted
	AutomaticFailoverPhaseCancelled
)

// String returns a stable phase name for status output.
func (phase AutomaticFailoverPhase) String() string {
	switch phase {
	case AutomaticFailoverPhaseIdle:
		return "idle"
	case AutomaticFailoverPhaseProposed:
		return "proposed"
	case AutomaticFailoverPhaseCommitted:
		return "committed"
	case AutomaticFailoverPhaseCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// AutomaticFailoverOptions configures one policy coordinator. Enabled is
// intentionally false by default. QuorumSize zero selects a strict majority of
// VoterCount in each trusted observation; MaxLag zero requires an exact journal
// sequence match.
type AutomaticFailoverOptions struct {
	Enabled       bool
	QuorumSize    int
	MaxLag        uint64
	MaxCandidates int
}

// AutomaticFailoverCandidate is a trusted health and replay observation for a
// possible replacement source.
type AutomaticFailoverCandidate struct {
	NodeID          string
	Healthy         bool
	AppliedSequence uint64
}

// AutomaticFailoverObservation is supplied by the embedding control plane. It
// must be authenticated and bound to one topology generation before evaluation.
type AutomaticFailoverObservation struct {
	SourceID           string
	SourceHealthy      bool
	SourceSequence     uint64
	FencingToken       uint64
	TopologyGeneration uint64
	VoterCount         int
	HealthyVoterCount  int
	Candidates         []AutomaticFailoverCandidate
}

// AutomaticFailoverDecision is a deterministic promotion proposal. It is a
// proposal only; callers still use their consensus/topology commit path and
// ReplicaPromotionBarrier to execute the promotion.
type AutomaticFailoverDecision struct {
	SourceID           string
	CandidateID        string
	SourceSequence     uint64
	CandidateSequence  uint64
	FencingToken       uint64
	TopologyGeneration uint64
	QuorumSize         int
	HealthyVoterCount  int
}

// AutomaticFailoverProposal binds a decision to one local lifecycle generation.
type AutomaticFailoverProposal struct {
	Generation uint64
	Decision   AutomaticFailoverDecision
}

// AutomaticFailoverState is an independent status snapshot. HasProposal is
// false before a proposal exists; the inline proposal keeps Snapshot allocation
// free for status and monitoring paths.
type AutomaticFailoverState struct {
	Phase       AutomaticFailoverPhase
	Generation  uint64
	HasProposal bool
	Proposal    AutomaticFailoverProposal
}

// AutomaticFailoverCoordinator evaluates one failover event and fences its
// commit. It has no background goroutine or network side effect. Construct a
// new coordinator for a later health event.
type AutomaticFailoverCoordinator struct {
	mu      sync.RWMutex
	options AutomaticFailoverOptions
	state   AutomaticFailoverState
}

// NewAutomaticFailoverCoordinator creates a coordinator. The zero-value
// options preserve the existing manual-only behavior.
func NewAutomaticFailoverCoordinator(options AutomaticFailoverOptions) (*AutomaticFailoverCoordinator, error) {
	normalized, err := normalizeAutomaticFailoverOptions(options)
	if err != nil {
		return nil, err
	}
	return &AutomaticFailoverCoordinator{
		options: normalized,
		state:   AutomaticFailoverState{Phase: AutomaticFailoverPhaseIdle},
	}, nil
}

// Propose evaluates one authenticated health observation and creates a single
// deterministic promotion proposal.
func (coordinator *AutomaticFailoverCoordinator) Propose(observation AutomaticFailoverObservation) (AutomaticFailoverProposal, error) {
	if coordinator == nil {
		return AutomaticFailoverProposal{}, ErrAutomaticFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != AutomaticFailoverPhaseIdle {
		return AutomaticFailoverProposal{}, ErrAutomaticFailoverAlreadyStarted
	}
	if !coordinator.options.Enabled {
		return AutomaticFailoverProposal{}, ErrAutomaticFailoverDisabled
	}
	decision, err := evaluateAutomaticFailover(coordinator.options, observation)
	if err != nil {
		return AutomaticFailoverProposal{}, err
	}
	proposal := AutomaticFailoverProposal{Generation: 1, Decision: decision}
	coordinator.state = AutomaticFailoverState{
		Phase:       AutomaticFailoverPhaseProposed,
		Generation:  proposal.Generation,
		HasProposal: true,
		Proposal:    proposal,
	}
	return proposal, nil
}

// Commit accepts exactly the current proposal. The caller should perform its
// external consensus/topology commit using the returned decision immediately
// after this local fence succeeds.
func (coordinator *AutomaticFailoverCoordinator) Commit(proposal AutomaticFailoverProposal) (AutomaticFailoverState, error) {
	if coordinator == nil {
		return AutomaticFailoverState{}, ErrAutomaticFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != AutomaticFailoverPhaseProposed || !coordinator.state.HasProposal {
		return AutomaticFailoverState{}, ErrAutomaticFailoverPhase
	}
	if proposal.Generation != coordinator.state.Generation {
		return AutomaticFailoverState{}, ErrAutomaticFailoverGeneration
	}
	if proposal.Decision != coordinator.state.Proposal.Decision {
		return AutomaticFailoverState{}, ErrAutomaticFailoverFencing
	}
	coordinator.state.Phase = AutomaticFailoverPhaseCommitted
	coordinator.state.Generation++
	return cloneAutomaticFailoverState(coordinator.state), nil
}

// Cancel invalidates a proposal before external promotion begins.
func (coordinator *AutomaticFailoverCoordinator) Cancel(expectedGeneration uint64) (AutomaticFailoverState, error) {
	if coordinator == nil {
		return AutomaticFailoverState{}, ErrAutomaticFailoverNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.state.Phase != AutomaticFailoverPhaseProposed {
		return AutomaticFailoverState{}, ErrAutomaticFailoverPhase
	}
	if expectedGeneration != coordinator.state.Generation {
		return AutomaticFailoverState{}, ErrAutomaticFailoverGeneration
	}
	coordinator.state.Phase = AutomaticFailoverPhaseCancelled
	coordinator.state.Generation++
	return cloneAutomaticFailoverState(coordinator.state), nil
}

// Snapshot returns a detached lifecycle view.
func (coordinator *AutomaticFailoverCoordinator) Snapshot() AutomaticFailoverState {
	if coordinator == nil {
		return AutomaticFailoverState{}
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return cloneAutomaticFailoverState(coordinator.state)
}

func evaluateAutomaticFailover(options AutomaticFailoverOptions, observation AutomaticFailoverObservation) (AutomaticFailoverDecision, error) {
	sourceID, err := normalizeAutomaticFailoverNodeID(observation.SourceID)
	if err != nil {
		return AutomaticFailoverDecision{}, err
	}
	if observation.SourceHealthy {
		return AutomaticFailoverDecision{}, ErrAutomaticFailoverSourceHealthy
	}
	if observation.FencingToken == 0 || observation.FencingToken == ^uint64(0) || observation.TopologyGeneration == ^uint64(0) {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: fencing or topology generation cannot advance", ErrAutomaticFailoverInvalid)
	}
	if observation.VoterCount < 1 || observation.HealthyVoterCount < 0 || observation.HealthyVoterCount > observation.VoterCount {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: voter counts are invalid", ErrAutomaticFailoverInvalid)
	}
	if len(observation.Candidates) == 0 || len(observation.Candidates) > options.MaxCandidates {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: candidate count is invalid", ErrAutomaticFailoverInvalid)
	}
	requiredQuorum := options.QuorumSize
	if requiredQuorum == 0 {
		requiredQuorum = observation.VoterCount/2 + 1
	}
	if requiredQuorum > observation.VoterCount {
		return AutomaticFailoverDecision{}, fmt.Errorf("%w: quorum exceeds voter count", ErrAutomaticFailoverInvalid)
	}
	if observation.HealthyVoterCount < requiredQuorum {
		return AutomaticFailoverDecision{}, ErrAutomaticFailoverNoQuorum
	}
	var selected AutomaticFailoverCandidate
	selectedFound := false
	seen := make(map[string]struct{}, len(observation.Candidates))
	for _, candidate := range observation.Candidates {
		nodeID, err := normalizeAutomaticFailoverNodeID(candidate.NodeID)
		if err != nil {
			return AutomaticFailoverDecision{}, err
		}
		if nodeID == sourceID {
			return AutomaticFailoverDecision{}, fmt.Errorf("%w: source is also a candidate", ErrAutomaticFailoverInvalid)
		}
		if _, exists := seen[nodeID]; exists {
			return AutomaticFailoverDecision{}, fmt.Errorf("%w: duplicate candidate %q", ErrAutomaticFailoverInvalid, nodeID)
		}
		seen[nodeID] = struct{}{}
		candidate.NodeID = nodeID
		if !candidate.Healthy || !automaticFailoverCandidateCaughtUp(observation.SourceSequence, candidate.AppliedSequence, options.MaxLag) {
			continue
		}
		if !selectedFound || candidate.AppliedSequence > selected.AppliedSequence || (candidate.AppliedSequence == selected.AppliedSequence && candidate.NodeID < selected.NodeID) {
			selected = candidate
			selectedFound = true
		}
	}
	if !selectedFound {
		return AutomaticFailoverDecision{}, ErrAutomaticFailoverNoCandidate
	}
	return AutomaticFailoverDecision{
		SourceID:           sourceID,
		CandidateID:        selected.NodeID,
		SourceSequence:     observation.SourceSequence,
		CandidateSequence:  selected.AppliedSequence,
		FencingToken:       observation.FencingToken + 1,
		TopologyGeneration: observation.TopologyGeneration + 1,
		QuorumSize:         requiredQuorum,
		HealthyVoterCount:  observation.HealthyVoterCount,
	}, nil
}

func automaticFailoverCandidateCaughtUp(sourceSequence, candidateSequence, maxLag uint64) bool {
	if candidateSequence >= sourceSequence {
		return true
	}
	return sourceSequence-candidateSequence <= maxLag
}

func normalizeAutomaticFailoverOptions(options AutomaticFailoverOptions) (AutomaticFailoverOptions, error) {
	if options.QuorumSize < 0 {
		return AutomaticFailoverOptions{}, fmt.Errorf("%w: quorum cannot be negative", ErrAutomaticFailoverInvalid)
	}
	if options.MaxCandidates == 0 {
		options.MaxCandidates = DefaultAutomaticFailoverMaxCandidates
	}
	if options.MaxCandidates < 1 || options.MaxCandidates > MaxAutomaticFailoverMaxCandidates {
		return AutomaticFailoverOptions{}, fmt.Errorf("%w: candidate limit is invalid", ErrAutomaticFailoverInvalid)
	}
	return options, nil
}

func normalizeAutomaticFailoverNodeID(nodeID string) (string, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || len(nodeID) > MaxAutomaticFailoverNodeIDBytes || strings.IndexByte(nodeID, 0) >= 0 {
		return "", fmt.Errorf("%w: node ID is invalid", ErrAutomaticFailoverInvalid)
	}
	return nodeID, nil
}

func cloneAutomaticFailoverState(state AutomaticFailoverState) AutomaticFailoverState {
	return state
}
