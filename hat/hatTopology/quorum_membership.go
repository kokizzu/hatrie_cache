package hatTopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// TopologyRolePrimary identifies a node eligible to own writes.
	TopologyRolePrimary = "primary"
	// TopologyRoleReplica identifies a normal quorum-participating replica.
	TopologyRoleReplica = "replica"
	// TopologyRoleAnonymous identifies a replica that never participates in
	// quorum decisions and cannot be selected as a shard primary.
	TopologyRoleAnonymous = "anonymous"
)

var (
	// ErrQuorumVoterIDsInvalid reports malformed topology quorum membership.
	ErrQuorumVoterIDsInvalid = errors.New("hatriecache: topology quorum voters are invalid")
)

// QuorumVoterIDs returns deterministic IDs for nodes that may participate in
// quorum decisions. Anonymous replicas remain valid topology members but are
// intentionally excluded. The input need not be normalized first; IDs and
// roles are trimmed and validated without mutating the input.
func QuorumVoterIDs(topology ClusterTopology) ([]string, error) {
	if len(topology.Nodes) == 0 {
		return nil, fmt.Errorf("%w: topology has no nodes", ErrQuorumVoterIDsInvalid)
	}
	seen := make(map[string]struct{}, len(topology.Nodes))
	voters := make([]string, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		id := strings.TrimSpace(node.ID)
		role := strings.TrimSpace(node.Role)
		if id == "" || !isValidTopologyRole(role) {
			return nil, fmt.Errorf("%w: node %q has invalid role or ID", ErrQuorumVoterIDsInvalid, id)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrQuorumVoterIDsInvalid, id)
		}
		seen[id] = struct{}{}
		if role != TopologyRoleAnonymous {
			voters = append(voters, id)
		}
	}
	if len(voters) == 0 {
		return nil, fmt.Errorf("%w: topology has no quorum voters", ErrQuorumVoterIDsInvalid)
	}
	sort.Strings(voters)
	return voters, nil
}

func isValidTopologyRole(role string) bool {
	return role == "" || role == TopologyRolePrimary || role == TopologyRoleReplica || role == TopologyRoleAnonymous
}
