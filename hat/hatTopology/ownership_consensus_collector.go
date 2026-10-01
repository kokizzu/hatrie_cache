package hatTopology

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrPartitionOwnershipConsensusPending indicates that a collector has not
	// reached quorum and could still change with another vote.
	ErrPartitionOwnershipConsensusPending = errors.New("hatriecache: partition ownership consensus is pending")
	// ErrPartitionOwnershipConsensusClosed indicates that a terminal collector
	// cannot accept more votes.
	ErrPartitionOwnershipConsensusClosed = errors.New("hatriecache: partition ownership consensus collector is closed")
)

const (
	partitionOwnershipConsensusVotePending uint8 = iota
	partitionOwnershipConsensusVoteAcknowledged
	partitionOwnershipConsensusVoteRejected
)

// PartitionOwnershipConsensusCollector incrementally collects votes for one
// ownership proposal. It is safe for concurrent vote producers. The collector
// retains one state byte and one index entry per voter, and never retains the
// full vote values after validation.
type PartitionOwnershipConsensusCollector struct {
	mu           sync.Mutex
	ownership    PartitionOwnership
	voters       []string
	voterIndex   map[string]int
	states       []uint8
	required     int
	seen         int
	acknowledged int
	rejected     int
	terminal     bool
}

// NewPartitionOwnershipConsensusCollector validates one proposal and creates
// an incremental quorum collector. The proposal and voter identities are
// copied and are not affected by later caller mutations.
func NewPartitionOwnershipConsensusCollector(policy TopologyConsensusPolicy, expected PartitionOwnership) (*PartitionOwnershipConsensusCollector, error) {
	if err := validatePartitionOwnershipConsensusMetadata(expected); err != nil {
		return nil, err
	}
	voters, _, required, err := preparePartitionOwnershipConsensusVoters(policy)
	if err != nil {
		return nil, err
	}
	voterIndex := make(map[string]int, len(voters))
	for index, voter := range voters {
		voterIndex[voter] = index
	}
	return &PartitionOwnershipConsensusCollector{
		ownership:  clonePartitionOwnershipConsensusMetadata(expected),
		voters:     append([]string(nil), voters...),
		voterIndex: voterIndex,
		states:     make([]uint8, len(voters)),
		required:   required,
	}, nil
}

// AddVote validates and records one vote. The returned terminal value is true
// when the collector has either reached quorum or can no longer reach it.
// A rejected or mismatched ownership vote is valid input and counts against
// the remaining possible quorum; malformed and duplicate votes return errors
// without changing collector state.
func (collector *PartitionOwnershipConsensusCollector) AddVote(vote PartitionOwnershipConsensusVote) (terminal bool, err error) {
	if collector == nil {
		return false, ErrPartitionOwnershipConsensusInvalid
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.terminal {
		return true, ErrPartitionOwnershipConsensusClosed
	}
	nodeID := strings.TrimSpace(vote.NodeID)
	if nodeID == "" {
		return false, fmt.Errorf("%w: vote node name is empty", ErrPartitionOwnershipConsensusInvalid)
	}
	index, ok := collector.voterIndex[nodeID]
	if !ok {
		return false, fmt.Errorf("%w: %q is not a voter", ErrPartitionOwnershipConsensusInvalid, nodeID)
	}
	if collector.states[index] != partitionOwnershipConsensusVotePending {
		return false, fmt.Errorf("%w: duplicate vote from %q", ErrPartitionOwnershipConsensusInvalid, nodeID)
	}
	if err := validatePartitionOwnershipConsensusMetadata(vote.Ownership); err != nil {
		return false, fmt.Errorf("%w: vote from %q: %v", ErrPartitionOwnershipConsensusInvalid, nodeID, err)
	}
	collector.seen++
	if vote.Accepted && partitionOwnershipConsensusMetadataEqual(collector.ownership, vote.Ownership) {
		collector.states[index] = partitionOwnershipConsensusVoteAcknowledged
		collector.acknowledged++
	} else {
		collector.states[index] = partitionOwnershipConsensusVoteRejected
		collector.rejected++
	}
	if collector.acknowledged >= collector.required || collector.acknowledged+len(collector.voters)-collector.seen < collector.required {
		collector.terminal = true
	}
	return collector.terminal, nil
}

// Finalize closes the collector with the votes received so far. Missing votes
// remain absent rather than being reported as rejections. This lets callers
// bound an external vote timeout without inventing negative acknowledgements.
func (collector *PartitionOwnershipConsensusCollector) Finalize() (PartitionOwnershipConsensusDecision, error) {
	if collector == nil {
		return PartitionOwnershipConsensusDecision{}, ErrPartitionOwnershipConsensusInvalid
	}
	collector.mu.Lock()
	collector.terminal = true
	decision := collector.decisionLocked()
	collector.mu.Unlock()
	return decision, nil
}

// Decision returns the terminal deterministic decision. Call Finalize to
// close a collector that has not reached quorum before all voters respond.
func (collector *PartitionOwnershipConsensusCollector) Decision() (PartitionOwnershipConsensusDecision, error) {
	if collector == nil {
		return PartitionOwnershipConsensusDecision{}, ErrPartitionOwnershipConsensusInvalid
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if !collector.terminal {
		return PartitionOwnershipConsensusDecision{}, ErrPartitionOwnershipConsensusPending
	}
	return collector.decisionLocked(), nil
}

// Complete reports whether the collector reached a terminal state.
func (collector *PartitionOwnershipConsensusCollector) Complete() bool {
	if collector == nil {
		return false
	}
	collector.mu.Lock()
	complete := collector.terminal
	collector.mu.Unlock()
	return complete
}

// Reset clears collected votes so the same proposal can be evaluated again.
// It is useful when a caller reuses one fixed ownership proposal for repeated
// membership rounds. Reset does not change the proposal, voter set, or quorum
// threshold.
func (collector *PartitionOwnershipConsensusCollector) Reset() error {
	if collector == nil {
		return ErrPartitionOwnershipConsensusInvalid
	}
	collector.mu.Lock()
	for index := range collector.states {
		collector.states[index] = partitionOwnershipConsensusVotePending
	}
	collector.seen = 0
	collector.acknowledged = 0
	collector.rejected = 0
	collector.terminal = false
	collector.mu.Unlock()
	return nil
}

func (collector *PartitionOwnershipConsensusCollector) decisionLocked() PartitionOwnershipConsensusDecision {
	acknowledged := make([]string, 0, collector.acknowledged)
	rejected := make([]string, 0, collector.rejected)
	for index, voter := range collector.voters {
		switch collector.states[index] {
		case partitionOwnershipConsensusVoteAcknowledged:
			acknowledged = append(acknowledged, voter)
		case partitionOwnershipConsensusVoteRejected:
			rejected = append(rejected, voter)
		}
	}
	return PartitionOwnershipConsensusDecision{
		Ownership:    clonePartitionOwnershipConsensusMetadata(collector.ownership),
		Voters:       append([]string(nil), collector.voters...),
		Required:     collector.required,
		Acknowledged: acknowledged,
		Rejected:     rejected,
		Satisfied:    collector.acknowledged >= collector.required,
	}
}
