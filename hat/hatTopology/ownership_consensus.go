package hatTopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrPartitionOwnershipConsensusInvalid     = errors.New("hatriecache: invalid partition ownership consensus")
	ErrPartitionOwnershipConsensusUnsatisfied = errors.New("hatriecache: partition ownership consensus is unsatisfied")
)

// PartitionOwnershipConsensusVote is one transport-neutral response for a
// partition ownership proposal. Accepted votes count only when their complete
// ownership metadata matches the expected snapshot.
type PartitionOwnershipConsensusVote struct {
	NodeID    string             `json:"node_id"`
	Ownership PartitionOwnership `json:"ownership"`
	Accepted  bool               `json:"accepted"`
}

// PartitionOwnershipConsensusDecision is the deterministic result of
// collecting ownership votes. The embedded snapshot binds a decision to one
// shard and fencing generation, so a decision cannot be reused for another
// ownership proposal.
type PartitionOwnershipConsensusDecision struct {
	Ownership    PartitionOwnership `json:"ownership"`
	Voters       []string           `json:"voters"`
	Required     int                `json:"required"`
	Acknowledged []string           `json:"acknowledged,omitempty"`
	Rejected     []string           `json:"rejected,omitempty"`
	Satisfied    bool               `json:"satisfied"`
}

// EvaluatePartitionOwnershipConsensus validates voters and produces a
// deterministic quorum decision for one partition ownership snapshot. It is
// transport-neutral; callers collect votes through HTTP, gRPC, or another
// control-plane channel. Missing votes do not count and are not reported as
// rejections.
func EvaluatePartitionOwnershipConsensus(policy TopologyConsensusPolicy, expected PartitionOwnership, votes []PartitionOwnershipConsensusVote) (PartitionOwnershipConsensusDecision, error) {
	if err := validatePartitionOwnershipConsensusMetadata(expected); err != nil {
		return PartitionOwnershipConsensusDecision{}, err
	}
	voters, voterSet, required, err := preparePartitionOwnershipConsensusVoters(policy)
	if err != nil {
		return PartitionOwnershipConsensusDecision{}, err
	}

	voteByNode := make(map[string]PartitionOwnershipConsensusVote, len(votes))
	for _, vote := range votes {
		nodeID := strings.TrimSpace(vote.NodeID)
		if nodeID == "" {
			return PartitionOwnershipConsensusDecision{}, fmt.Errorf("%w: vote node name is empty", ErrPartitionOwnershipConsensusInvalid)
		}
		if _, exists := voterSet[nodeID]; !exists {
			return PartitionOwnershipConsensusDecision{}, fmt.Errorf("%w: %q is not a voter", ErrPartitionOwnershipConsensusInvalid, nodeID)
		}
		if _, exists := voteByNode[nodeID]; exists {
			return PartitionOwnershipConsensusDecision{}, fmt.Errorf("%w: duplicate vote from %q", ErrPartitionOwnershipConsensusInvalid, nodeID)
		}
		if err := validatePartitionOwnershipConsensusMetadata(vote.Ownership); err != nil {
			return PartitionOwnershipConsensusDecision{}, fmt.Errorf("%w: vote from %q: %v", ErrPartitionOwnershipConsensusInvalid, nodeID, err)
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
		if vote.Accepted && partitionOwnershipConsensusMetadataEqual(expected, vote.Ownership) {
			acknowledged = append(acknowledged, nodeID)
		} else {
			rejected = append(rejected, nodeID)
		}
	}

	return PartitionOwnershipConsensusDecision{
		Ownership:    clonePartitionOwnershipConsensusMetadata(expected),
		Voters:       voters,
		Required:     required,
		Acknowledged: acknowledged,
		Rejected:     rejected,
		Satisfied:    len(acknowledged) >= required,
	}, nil
}

// ValidatePartitionOwnershipConsensusDecision verifies that a satisfied
// decision is bound to the supplied ownership snapshot and contains no
// duplicate or unknown acknowledgements.
func ValidatePartitionOwnershipConsensusDecision(decision PartitionOwnershipConsensusDecision, expected PartitionOwnership) error {
	if !decision.Satisfied {
		return ErrPartitionOwnershipConsensusUnsatisfied
	}
	if err := validatePartitionOwnershipConsensusMetadata(expected); err != nil {
		return err
	}
	if err := validatePartitionOwnershipConsensusMetadata(decision.Ownership); err != nil {
		return fmt.Errorf("%w: decision ownership: %v", ErrPartitionOwnershipConsensusInvalid, err)
	}
	if !partitionOwnershipConsensusMetadataEqual(expected, decision.Ownership) {
		return fmt.Errorf("%w: decision ownership does not match proposal", ErrPartitionOwnershipConsensusInvalid)
	}
	if len(decision.Voters) == 0 || decision.Required < 1 || decision.Required > len(decision.Voters) {
		return fmt.Errorf("%w: voter threshold is invalid", ErrPartitionOwnershipConsensusInvalid)
	}
	voterSet := make(map[string]struct{}, len(decision.Voters))
	for _, voter := range decision.Voters {
		voter = strings.TrimSpace(voter)
		if voter == "" {
			return fmt.Errorf("%w: voter name is empty", ErrPartitionOwnershipConsensusInvalid)
		}
		if _, exists := voterSet[voter]; exists {
			return fmt.Errorf("%w: duplicate voter %q", ErrPartitionOwnershipConsensusInvalid, voter)
		}
		voterSet[voter] = struct{}{}
	}
	acknowledged, err := validatePartitionOwnershipConsensusMembers("acknowledged", decision.Acknowledged, voterSet)
	if err != nil {
		return err
	}
	rejected, err := validatePartitionOwnershipConsensusMembers("rejected", decision.Rejected, voterSet)
	if err != nil {
		return err
	}
	for voter := range acknowledged {
		if _, exists := rejected[voter]; exists {
			return fmt.Errorf("%w: voter %q is both acknowledged and rejected", ErrPartitionOwnershipConsensusInvalid, voter)
		}
	}
	if len(acknowledged) < decision.Required {
		return fmt.Errorf("%w: acknowledgement threshold is not satisfied", ErrPartitionOwnershipConsensusInvalid)
	}
	return nil
}

func preparePartitionOwnershipConsensusVoters(policy TopologyConsensusPolicy) ([]string, map[string]struct{}, int, error) {
	if len(policy.Voters) == 0 {
		return nil, nil, 0, fmt.Errorf("%w: voters are empty", ErrPartitionOwnershipConsensusInvalid)
	}
	voters := make([]string, 0, len(policy.Voters))
	voterSet := make(map[string]struct{}, len(policy.Voters))
	for _, voter := range policy.Voters {
		voter = strings.TrimSpace(voter)
		if voter == "" {
			return nil, nil, 0, fmt.Errorf("%w: voter name is empty", ErrPartitionOwnershipConsensusInvalid)
		}
		if _, exists := voterSet[voter]; exists {
			return nil, nil, 0, fmt.Errorf("%w: duplicate voter %q", ErrPartitionOwnershipConsensusInvalid, voter)
		}
		voterSet[voter] = struct{}{}
		voters = append(voters, voter)
	}
	sort.Strings(voters)
	required := policy.Required
	if required == 0 {
		required = len(voters)/2 + 1
	}
	if required < 1 || required > len(voters) {
		return nil, nil, 0, fmt.Errorf("%w: required acknowledgements %d outside 1..%d", ErrPartitionOwnershipConsensusInvalid, required, len(voters))
	}
	return voters, voterSet, required, nil
}

func validatePartitionOwnershipConsensusMetadata(ownership PartitionOwnership) error {
	if strings.TrimSpace(ownership.Primary) == "" {
		return fmt.Errorf("%w: primary is required", ErrPartitionOwnershipConsensusInvalid)
	}
	if strings.TrimSpace(ownership.TopologyFingerprint) == "" {
		return fmt.Errorf("%w: topology fingerprint is required", ErrPartitionOwnershipConsensusInvalid)
	}
	for index, replica := range ownership.Replicas {
		replica = strings.TrimSpace(replica)
		if replica == "" {
			return fmt.Errorf("%w: replica name is empty", ErrPartitionOwnershipConsensusInvalid)
		}
		if replica == ownership.Primary {
			return fmt.Errorf("%w: primary is also a replica", ErrPartitionOwnershipConsensusInvalid)
		}
		for _, previous := range ownership.Replicas[:index] {
			if replica == strings.TrimSpace(previous) {
				return fmt.Errorf("%w: duplicate replica %q", ErrPartitionOwnershipConsensusInvalid, replica)
			}
		}
	}
	return nil
}

func partitionOwnershipConsensusMetadataEqual(left, right PartitionOwnership) bool {
	if left.ShardID != right.ShardID || left.Primary != right.Primary || left.TopologyFingerprint != right.TopologyFingerprint || left.FencingToken != right.FencingToken || len(left.Replicas) != len(right.Replicas) {
		return false
	}
	for index := range left.Replicas {
		if left.Replicas[index] != right.Replicas[index] {
			return false
		}
	}
	return true
}

func clonePartitionOwnershipConsensusMetadata(ownership PartitionOwnership) PartitionOwnership {
	ownership.Replicas = append([]string(nil), ownership.Replicas...)
	return ownership
}

func validatePartitionOwnershipConsensusMembers(kind string, members []string, voters map[string]struct{}) (map[string]struct{}, error) {
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			return nil, fmt.Errorf("%w: %s voter name is empty", ErrPartitionOwnershipConsensusInvalid, kind)
		}
		if _, exists := seen[member]; exists {
			return nil, fmt.Errorf("%w: duplicate %s voter %q", ErrPartitionOwnershipConsensusInvalid, kind, member)
		}
		if _, exists := voters[member]; !exists {
			return nil, fmt.Errorf("%w: %s voter %q is not a voter", ErrPartitionOwnershipConsensusInvalid, kind, member)
		}
		seen[member] = struct{}{}
	}
	return seen, nil
}
