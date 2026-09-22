package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultReplicaSetLeaderElectionTimeout    = 5 * time.Second
	DefaultReplicaSetLeaderElectionMaxMembers = 64
	maxReplicaSetLeaderElectionNodeBytes      = 256
)

var (
	ErrReplicaSetLeaderElectionNil           = errors.New("hatReplication: replica-set leader election is nil")
	ErrReplicaSetLeaderElectionInvalid       = errors.New("hatReplication: replica-set leader election options are invalid")
	ErrReplicaSetLeaderElectionDisabled      = errors.New("hatReplication: replica-set leader election is disabled")
	ErrReplicaSetLeaderElectionUnknownVoter  = errors.New("hatReplication: replica-set leader election voter is unknown")
	ErrReplicaSetLeaderElectionObservation   = errors.New("hatReplication: replica-set leader election heartbeat is invalid")
	ErrReplicaSetLeaderElectionNoQuorum      = errors.New("hatReplication: replica-set leader election quorum is unavailable")
	ErrReplicaSetLeaderElectionNoCandidate   = errors.New("hatReplication: replica-set leader election has no eligible candidate")
	ErrReplicaSetLeaderElectionLeaderHealthy = errors.New("hatReplication: replica-set leader is still healthy")
	ErrReplicaSetLeaderElectionPending       = errors.New("hatReplication: replica-set leader election proposal is pending")
	ErrReplicaSetLeaderElectionNoPending     = errors.New("hatReplication: replica-set leader election has no pending proposal")
	ErrReplicaSetLeaderElectionStale         = errors.New("hatReplication: replica-set leader election proposal is stale")
	ErrReplicaSetLeaderElectionCandidateLate = errors.New("hatReplication: replica-set leader election candidate is no longer ready")
	ErrReplicaSetLeaderElectionGeneration    = errors.New("hatReplication: replica-set leader election generation is exhausted")
)

// ReplicaSetLeaderElectionOptions configures a fixed voter set. Enabled is
// false by default; callers must opt into automatic decisions explicitly.
type ReplicaSetLeaderElectionOptions struct {
	Enabled         bool
	Voters          []string
	QuorumSize      int
	ElectionTimeout time.Duration
	InitialLeader   string
	InitialTerm     uint64
	MaxMembers      int
}

// ReplicaSetLeaderHeartbeat is the authenticated local observation for one
// voter. ObserveHeartbeat supplies the observation time, so remote nodes
// cannot extend their own liveness by sending a future timestamp.
type ReplicaSetLeaderHeartbeat struct {
	NodeID          string
	AppliedSequence uint64
	Healthy         bool
}

// ReplicaSetLeaderElectionProposal is an immutable, quorum-qualified leader
// choice. Commit must receive the exact proposal returned by TryElect.
type ReplicaSetLeaderElectionProposal struct {
	Term            uint64
	Generation      uint64
	FencingToken    uint64
	LeaderID        string
	AppliedSequence uint64
	RequiredQuorum  int
	HealthyVoters   int
}

// ReplicaSetLeaderElectionState is a detached lifecycle state. A pending
// proposal is not serving leadership until Commit succeeds.
type ReplicaSetLeaderElectionState struct {
	Term         uint64
	LeaderID     string
	FencingToken uint64
	Generation   uint64
	HasPending   bool
	Pending      ReplicaSetLeaderElectionProposal
}

// ReplicaSetLeaderObservation is a detached heartbeat view used by Snapshot.
type ReplicaSetLeaderObservation struct {
	NodeID          string
	AppliedSequence uint64
	Healthy         bool
	LastSeen        time.Time
}

// ReplicaSetLeaderElectionSnapshot is a deterministic status view of the
// voter set, lifecycle state, and latest heartbeat observations.
type ReplicaSetLeaderElectionSnapshot struct {
	State           ReplicaSetLeaderElectionState
	Voters          []string
	Observations    []ReplicaSetLeaderObservation
	QuorumSize      int
	ElectionTimeout time.Duration
}

type replicaSetLeaderObservation struct {
	ReplicaSetLeaderHeartbeat
	lastSeen time.Time
}

// ReplicaSetLeaderElection collects bounded voter heartbeats and creates a
// deterministic election proposal after the current leader expires. It has no
// background goroutine, network side effect, or implicit commit; callers run
// TryElect from their health/timer loop and commit through their consensus or
// topology path.
type ReplicaSetLeaderElection struct {
	mu              sync.RWMutex
	enabled         bool
	voters          []string
	voterSet        map[string]struct{}
	quorumSize      int
	electionTimeout time.Duration
	observations    map[string]replicaSetLeaderObservation
	term            uint64
	leaderID        string
	fencingToken    uint64
	generation      uint64
	pending         *ReplicaSetLeaderElectionProposal
}

// NewReplicaSetLeaderElection validates and constructs a fixed-voter election
// state machine. A zero quorum selects a strict majority.
func NewReplicaSetLeaderElection(options ReplicaSetLeaderElectionOptions) (*ReplicaSetLeaderElection, error) {
	maxMembers := options.MaxMembers
	if maxMembers == 0 {
		maxMembers = DefaultReplicaSetLeaderElectionMaxMembers
	}
	if maxMembers < 1 || maxMembers > 1<<20 || len(options.Voters) == 0 || len(options.Voters) > maxMembers {
		return nil, ErrReplicaSetLeaderElectionInvalid
	}
	voters, voterSet, err := normalizeReplicaSetLeaderVoters(options.Voters)
	if err != nil {
		return nil, err
	}
	quorumSize := options.QuorumSize
	if quorumSize == 0 {
		quorumSize = len(voters)/2 + 1
	}
	if quorumSize < 1 || quorumSize > len(voters) {
		return nil, fmt.Errorf("%w: quorum size is invalid", ErrReplicaSetLeaderElectionInvalid)
	}
	timeout := options.ElectionTimeout
	if timeout == 0 {
		timeout = DefaultReplicaSetLeaderElectionTimeout
	}
	if timeout < 0 {
		return nil, fmt.Errorf("%w: election timeout is invalid", ErrReplicaSetLeaderElectionInvalid)
	}
	initialLeader := strings.TrimSpace(options.InitialLeader)
	if initialLeader != "" {
		if _, exists := voterSet[initialLeader]; !exists || options.InitialTerm == 0 {
			return nil, fmt.Errorf("%w: initial leader and term are inconsistent", ErrReplicaSetLeaderElectionInvalid)
		}
	} else if options.InitialTerm != 0 {
		return nil, fmt.Errorf("%w: initial term requires an initial leader", ErrReplicaSetLeaderElectionInvalid)
	}
	return &ReplicaSetLeaderElection{
		enabled:         options.Enabled,
		voters:          voters,
		voterSet:        voterSet,
		quorumSize:      quorumSize,
		electionTimeout: timeout,
		observations:    make(map[string]replicaSetLeaderObservation, len(voters)),
		term:            options.InitialTerm,
		leaderID:        initialLeader,
		fencingToken:    options.InitialTerm,
		generation:      1,
	}, nil
}

// Enabled reports whether TryElect and ObserveHeartbeat can change election
// state.
func (election *ReplicaSetLeaderElection) Enabled() bool {
	return election != nil && election.enabled
}

// ObserveHeartbeat records a monotone heartbeat for one configured voter.
// Older observation times or applied sequences are rejected instead of
// allowing a restarted or delayed node to move progress backward.
func (election *ReplicaSetLeaderElection) ObserveHeartbeat(now time.Time, heartbeat ReplicaSetLeaderHeartbeat) error {
	if election == nil {
		return ErrReplicaSetLeaderElectionNil
	}
	if !election.enabled {
		return ErrReplicaSetLeaderElectionDisabled
	}
	if now.IsZero() {
		return fmt.Errorf("%w: observation time is zero", ErrReplicaSetLeaderElectionObservation)
	}
	nodeID := strings.TrimSpace(heartbeat.NodeID)
	if nodeID == "" || len(nodeID) > maxReplicaSetLeaderElectionNodeBytes {
		return fmt.Errorf("%w: node ID is invalid", ErrReplicaSetLeaderElectionObservation)
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if _, exists := election.voterSet[nodeID]; !exists {
		return fmt.Errorf("%w: %s", ErrReplicaSetLeaderElectionUnknownVoter, nodeID)
	}
	previous, exists := election.observations[nodeID]
	if exists {
		if now.Before(previous.lastSeen) || heartbeat.AppliedSequence < previous.AppliedSequence {
			return fmt.Errorf("%w: voter %s moved backward", ErrReplicaSetLeaderElectionObservation, nodeID)
		}
	}
	heartbeat.NodeID = nodeID
	election.observations[nodeID] = replicaSetLeaderObservation{ReplicaSetLeaderHeartbeat: heartbeat, lastSeen: now}
	return nil
}

// TryElect creates one proposal when the current leader is absent or its last
// healthy heartbeat has expired. The healthiest quorum is selected by highest
// applied sequence, then by node ID for deterministic ties.
func (election *ReplicaSetLeaderElection) TryElect(now time.Time) (ReplicaSetLeaderElectionProposal, error) {
	if election == nil {
		return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionNil
	}
	if !election.enabled {
		return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionDisabled
	}
	if now.IsZero() {
		return ReplicaSetLeaderElectionProposal{}, fmt.Errorf("%w: election time is zero", ErrReplicaSetLeaderElectionInvalid)
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if election.pending != nil {
		return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionPending
	}
	if election.leaderID != "" {
		if observation, exists := election.observations[election.leaderID]; exists && observation.Healthy && replicaSetLeaderFresh(now, observation.lastSeen, election.electionTimeout) {
			return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionLeaderHealthy
		}
	}
	healthyVoters := 0
	var selected replicaSetLeaderObservation
	selectedNode := ""
	for _, nodeID := range election.voters {
		observation, exists := election.observations[nodeID]
		if !exists || !observation.Healthy || !replicaSetLeaderFresh(now, observation.lastSeen, election.electionTimeout) {
			continue
		}
		healthyVoters++
		if selectedNode == "" || observation.AppliedSequence > selected.AppliedSequence || (observation.AppliedSequence == selected.AppliedSequence && nodeID < selectedNode) {
			selected = observation
			selectedNode = nodeID
		}
	}
	if healthyVoters < election.quorumSize {
		return ReplicaSetLeaderElectionProposal{}, fmt.Errorf("%w: healthy=%d required=%d", ErrReplicaSetLeaderElectionNoQuorum, healthyVoters, election.quorumSize)
	}
	if selectedNode == "" {
		return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionNoCandidate
	}
	if election.term == ^uint64(0) || election.generation == ^uint64(0) {
		return ReplicaSetLeaderElectionProposal{}, ErrReplicaSetLeaderElectionGeneration
	}
	proposal := ReplicaSetLeaderElectionProposal{
		Term:            election.term + 1,
		Generation:      election.generation,
		FencingToken:    election.term + 1,
		LeaderID:        selectedNode,
		AppliedSequence: selected.AppliedSequence,
		RequiredQuorum:  election.quorumSize,
		HealthyVoters:   healthyVoters,
	}
	election.pending = &proposal
	return proposal, nil
}

// Commit accepts the exact current proposal after rechecking that its chosen
// candidate is still healthy, fresh, and at least as caught up as proposed.
func (election *ReplicaSetLeaderElection) Commit(now time.Time, proposal ReplicaSetLeaderElectionProposal) (ReplicaSetLeaderElectionState, error) {
	if election == nil {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionNil
	}
	if !election.enabled {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionDisabled
	}
	if now.IsZero() {
		return ReplicaSetLeaderElectionState{}, fmt.Errorf("%w: commit time is zero", ErrReplicaSetLeaderElectionInvalid)
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if election.pending == nil {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionNoPending
	}
	if *election.pending != proposal || proposal.Generation != election.generation || proposal.Term <= election.term {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionStale
	}
	observation, exists := election.observations[proposal.LeaderID]
	if !exists || !observation.Healthy || !replicaSetLeaderFresh(now, observation.lastSeen, election.electionTimeout) || observation.AppliedSequence < proposal.AppliedSequence {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionCandidateLate
	}
	if election.generation == ^uint64(0) {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionGeneration
	}
	election.term = proposal.Term
	election.leaderID = proposal.LeaderID
	election.fencingToken = proposal.FencingToken
	election.generation++
	election.pending = nil
	return election.stateLocked(), nil
}

// Cancel discards the exact pending proposal and advances the local generation
// so a caller cannot accidentally commit the cancelled election later.
func (election *ReplicaSetLeaderElection) Cancel(proposal ReplicaSetLeaderElectionProposal) (ReplicaSetLeaderElectionState, error) {
	if election == nil {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionNil
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if election.pending == nil {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionNoPending
	}
	if *election.pending != proposal || proposal.Generation != election.generation {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionStale
	}
	if election.generation == ^uint64(0) {
		return ReplicaSetLeaderElectionState{}, ErrReplicaSetLeaderElectionGeneration
	}
	election.generation++
	election.pending = nil
	return election.stateLocked(), nil
}

// Snapshot returns a detached node-sorted view of election state.
func (election *ReplicaSetLeaderElection) Snapshot() ReplicaSetLeaderElectionSnapshot {
	if election == nil {
		return ReplicaSetLeaderElectionSnapshot{}
	}
	election.mu.RLock()
	defer election.mu.RUnlock()
	snapshot := ReplicaSetLeaderElectionSnapshot{
		State:           election.stateLocked(),
		Voters:          append([]string(nil), election.voters...),
		QuorumSize:      election.quorumSize,
		ElectionTimeout: election.electionTimeout,
		Observations:    make([]ReplicaSetLeaderObservation, 0, len(election.observations)),
	}
	for _, nodeID := range election.voters {
		observation, exists := election.observations[nodeID]
		if !exists {
			continue
		}
		snapshot.Observations = append(snapshot.Observations, ReplicaSetLeaderObservation{
			NodeID:          observation.NodeID,
			AppliedSequence: observation.AppliedSequence,
			Healthy:         observation.Healthy,
			LastSeen:        observation.lastSeen,
		})
	}
	return snapshot
}

func (election *ReplicaSetLeaderElection) stateLocked() ReplicaSetLeaderElectionState {
	state := ReplicaSetLeaderElectionState{
		Term:         election.term,
		LeaderID:     election.leaderID,
		FencingToken: election.fencingToken,
		Generation:   election.generation,
	}
	if election.pending != nil {
		state.HasPending = true
		state.Pending = *election.pending
	}
	return state
}

func replicaSetLeaderFresh(now, lastSeen time.Time, timeout time.Duration) bool {
	return !now.Before(lastSeen) && now.Sub(lastSeen) <= timeout
}

func normalizeReplicaSetLeaderVoters(voters []string) ([]string, map[string]struct{}, error) {
	normalized := make([]string, len(voters))
	seen := make(map[string]struct{}, len(voters))
	for index, voter := range voters {
		voter = strings.TrimSpace(voter)
		if voter == "" || len(voter) > maxReplicaSetLeaderElectionNodeBytes {
			return nil, nil, fmt.Errorf("%w: voter ID is invalid", ErrReplicaSetLeaderElectionInvalid)
		}
		if _, exists := seen[voter]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate voter %q", ErrReplicaSetLeaderElectionInvalid, voter)
		}
		seen[voter] = struct{}{}
		normalized[index] = voter
	}
	sort.Strings(normalized)
	return normalized, seen, nil
}
