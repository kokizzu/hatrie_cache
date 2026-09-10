package hatTopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrTopologyCommitInvalid        = errors.New("hatriecache: invalid topology commit")
	ErrTopologyCommitConflict       = errors.New("hatriecache: topology commit conflicts with current topology")
	ErrTopologyCommitStale          = errors.New("hatriecache: topology commit is stale")
	ErrTopologyConsensusInvalid     = errors.New("hatriecache: invalid topology consensus decision")
	ErrTopologyConsensusUnsatisfied = errors.New("hatriecache: topology consensus is unsatisfied")
)

// TopologyCommit is a CAS-style topology proposal. The expected fingerprint
// binds the proposal to the topology snapshot from which it was created.
type TopologyCommit struct {
	ExpectedFingerprint string          `json:"expected_fingerprint"`
	Topology            ClusterTopology `json:"topology"`
}

// NewTopologyCommit validates and snapshots a topology proposal.
func NewTopologyCommit(expectedFingerprint string, topology ClusterTopology) (TopologyCommit, error) {
	expectedFingerprint = strings.TrimSpace(expectedFingerprint)
	if expectedFingerprint == "" {
		return TopologyCommit{}, fmt.Errorf("%w: expected fingerprint is empty", ErrTopologyCommitInvalid)
	}
	normalized, err := Normalize(topology)
	if err != nil {
		return TopologyCommit{}, fmt.Errorf("%w: topology: %v", ErrTopologyCommitInvalid, err)
	}
	return TopologyCommit{ExpectedFingerprint: expectedFingerprint, Topology: normalized}, nil
}

// CandidateFingerprint returns the normalized fingerprint carried by the
// proposal, or an empty string for an invalid zero-value proposal.
func (commit TopologyCommit) CandidateFingerprint() string {
	normalized, err := Normalize(commit.Topology)
	if err != nil {
		return ""
	}
	return fingerprintNormalized(normalized)
}

// Validate checks a proposal against the current topology. Reapplying an
// already-installed candidate is valid, which makes retries idempotent.
func (commit TopologyCommit) Validate(current ClusterTopology) error {
	return ValidateTopologyCommit(current, commit)
}

// ValidateTopologyCommit checks the expected fingerprint and fencing-token
// monotonicity without mutating either topology.
func ValidateTopologyCommit(current ClusterTopology, commit TopologyCommit) error {
	expected := strings.TrimSpace(commit.ExpectedFingerprint)
	if expected == "" {
		return fmt.Errorf("%w: expected fingerprint is empty", ErrTopologyCommitInvalid)
	}
	currentNormalized, err := Normalize(current)
	if err != nil {
		return fmt.Errorf("%w: current topology: %v", ErrTopologyCommitInvalid, err)
	}
	candidateNormalized, err := Normalize(commit.Topology)
	if err != nil {
		return fmt.Errorf("%w: candidate topology: %v", ErrTopologyCommitInvalid, err)
	}
	currentFingerprint := fingerprintNormalized(currentNormalized)
	candidateFingerprint := fingerprintNormalized(candidateNormalized)
	if candidateFingerprint == currentFingerprint {
		return nil
	}
	if expected != currentFingerprint {
		return fmt.Errorf("%w: expected %q, current %q", ErrTopologyCommitConflict, expected, currentFingerprint)
	}
	if candidateNormalized.FencingToken <= currentNormalized.FencingToken {
		return fmt.Errorf("%w: fencing token %d is not newer than %d", ErrTopologyCommitStale, candidateNormalized.FencingToken, currentNormalized.FencingToken)
	}
	return nil
}

// TopologyConsensusPolicy identifies the voters and acknowledgement threshold
// for one proposal. Required zero means a strict majority of voters.
type TopologyConsensusPolicy struct {
	Voters   []string `json:"voters"`
	Required int      `json:"required,omitempty"`
}

// TopologyConsensusVote is a response from one voter. A vote is counted only
// when it binds to both fingerprints supplied to EvaluateTopologyConsensus.
type TopologyConsensusVote struct {
	NodeID               string `json:"node_id"`
	ExpectedFingerprint  string `json:"expected_fingerprint"`
	CandidateFingerprint string `json:"candidate_fingerprint"`
	Accepted             bool   `json:"accepted"`
}

// TopologyConsensusDecision is the deterministic result of collecting votes.
// The fingerprints make it unsafe to reuse a decision for another proposal.
type TopologyConsensusDecision struct {
	ExpectedFingerprint  string   `json:"expected_fingerprint"`
	CandidateFingerprint string   `json:"candidate_fingerprint"`
	Voters               []string `json:"voters"`
	Required             int      `json:"required"`
	Acknowledged         []string `json:"acknowledged,omitempty"`
	Rejected             []string `json:"rejected,omitempty"`
	Satisfied            bool     `json:"satisfied"`
}

// EvaluateTopologyConsensus validates voters and produces a deterministic
// quorum decision. It is transport-neutral; callers collect votes through
// HTTP, gRPC, or another control-plane channel.
func EvaluateTopologyConsensus(policy TopologyConsensusPolicy, expectedFingerprint, candidateFingerprint string, votes []TopologyConsensusVote) (TopologyConsensusDecision, error) {
	expectedFingerprint = strings.TrimSpace(expectedFingerprint)
	candidateFingerprint = strings.TrimSpace(candidateFingerprint)
	if expectedFingerprint == "" || candidateFingerprint == "" {
		return TopologyConsensusDecision{}, fmt.Errorf("%w: fingerprints are required", ErrTopologyConsensusInvalid)
	}

	voters := make([]string, 0, len(policy.Voters))
	voterSet := make(map[string]struct{}, len(policy.Voters))
	for _, voter := range policy.Voters {
		voter = strings.TrimSpace(voter)
		if voter == "" {
			return TopologyConsensusDecision{}, fmt.Errorf("%w: voter name is empty", ErrTopologyConsensusInvalid)
		}
		if _, exists := voterSet[voter]; exists {
			return TopologyConsensusDecision{}, fmt.Errorf("%w: duplicate voter %q", ErrTopologyConsensusInvalid, voter)
		}
		voterSet[voter] = struct{}{}
		voters = append(voters, voter)
	}
	if len(voters) == 0 {
		return TopologyConsensusDecision{}, fmt.Errorf("%w: voters are empty", ErrTopologyConsensusInvalid)
	}
	sort.Strings(voters)
	required := policy.Required
	if required == 0 {
		required = len(voters)/2 + 1
	}
	if required < 1 || required > len(voters) {
		return TopologyConsensusDecision{}, fmt.Errorf("%w: required acknowledgements %d outside 1..%d", ErrTopologyConsensusInvalid, required, len(voters))
	}

	voteByNode := make(map[string]TopologyConsensusVote, len(votes))
	for _, vote := range votes {
		nodeID := strings.TrimSpace(vote.NodeID)
		if nodeID == "" {
			return TopologyConsensusDecision{}, fmt.Errorf("%w: vote node name is empty", ErrTopologyConsensusInvalid)
		}
		if _, exists := voterSet[nodeID]; !exists {
			return TopologyConsensusDecision{}, fmt.Errorf("%w: %q is not a voter", ErrTopologyConsensusInvalid, nodeID)
		}
		if _, exists := voteByNode[nodeID]; exists {
			return TopologyConsensusDecision{}, fmt.Errorf("%w: duplicate vote from %q", ErrTopologyConsensusInvalid, nodeID)
		}
		voteByNode[nodeID] = vote
	}

	acknowledged := make([]string, 0, len(voteByNode))
	rejected := make([]string, 0, len(voteByNode))
	for _, nodeID := range voters {
		vote, exists := voteByNode[nodeID]
		if !exists {
			continue
		}
		if vote.Accepted && strings.TrimSpace(vote.ExpectedFingerprint) == expectedFingerprint && strings.TrimSpace(vote.CandidateFingerprint) == candidateFingerprint {
			acknowledged = append(acknowledged, nodeID)
		} else {
			rejected = append(rejected, nodeID)
		}
	}
	return TopologyConsensusDecision{
		ExpectedFingerprint:  expectedFingerprint,
		CandidateFingerprint: candidateFingerprint,
		Voters:               voters,
		Required:             required,
		Acknowledged:         acknowledged,
		Rejected:             rejected,
		Satisfied:            len(acknowledged) >= required,
	}, nil
}

// ValidateTopologyConsensusDecision verifies that a quorum decision is bound
// to one proposal and cannot satisfy its threshold with duplicate voters.
func ValidateTopologyConsensusDecision(decision TopologyConsensusDecision, expectedFingerprint, candidateFingerprint string) error {
	if !decision.Satisfied {
		return ErrTopologyConsensusUnsatisfied
	}
	expectedFingerprint = strings.TrimSpace(expectedFingerprint)
	candidateFingerprint = strings.TrimSpace(candidateFingerprint)
	if decision.ExpectedFingerprint != expectedFingerprint || decision.CandidateFingerprint != candidateFingerprint {
		return fmt.Errorf("%w: decision fingerprints do not match proposal", ErrTopologyConsensusInvalid)
	}
	if decision.Required < 1 || len(decision.Acknowledged) < decision.Required {
		return fmt.Errorf("%w: acknowledgement threshold is not satisfied", ErrTopologyConsensusInvalid)
	}
	if len(decision.Voters) == 0 || decision.Required > len(decision.Voters) {
		return fmt.Errorf("%w: voter threshold is invalid", ErrTopologyConsensusInvalid)
	}
	voters := make(map[string]struct{}, len(decision.Voters))
	for _, nodeID := range decision.Voters {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			return fmt.Errorf("%w: voter name is empty", ErrTopologyConsensusInvalid)
		}
		if _, exists := voters[nodeID]; exists {
			return fmt.Errorf("%w: duplicate voter %q", ErrTopologyConsensusInvalid, nodeID)
		}
		voters[nodeID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(decision.Acknowledged))
	for _, nodeID := range decision.Acknowledged {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			return fmt.Errorf("%w: acknowledged voter name is empty", ErrTopologyConsensusInvalid)
		}
		if _, exists := seen[nodeID]; exists {
			return fmt.Errorf("%w: duplicate acknowledged voter %q", ErrTopologyConsensusInvalid, nodeID)
		}
		if _, exists := voters[nodeID]; !exists {
			return fmt.Errorf("%w: acknowledged node %q is not a voter", ErrTopologyConsensusInvalid, nodeID)
		}
		seen[nodeID] = struct{}{}
	}
	return nil
}
