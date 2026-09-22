package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrReplicaRoleInvalid identifies an unsupported membership role.
	ErrReplicaRoleInvalid = errors.New("hatReplication: replica role is invalid")
	// ErrReplicaRoleRosterInvalid identifies malformed or overlapping role lists.
	ErrReplicaRoleRosterInvalid = errors.New("hatReplication: replica role roster is invalid")
	// ErrReplicaQuorumAnonymousTarget prevents an anonymous replica from being
	// counted as a quorum participant.
	ErrReplicaQuorumAnonymousTarget = errors.New("hatReplication: anonymous replica cannot participate in quorum")
	// ErrReplicaQuorumUnknownTarget prevents a node outside the current roster
	// from being counted as a quorum participant.
	ErrReplicaQuorumUnknownTarget = errors.New("hatReplication: quorum target is not a voter")
	// ErrReplicaJoinAdmissionRoleMismatch prevents a rejoin from changing a
	// member's quorum role without a separate membership transition.
	ErrReplicaJoinAdmissionRoleMismatch = errors.New("hatReplication: replica rejoin role does not match eviction")
)

// ReplicaRole determines whether an active member can participate in quorum
// decisions. The zero value remains a voter for compatibility with existing
// join requests; anonymous replicas are an explicit opt-in.
type ReplicaRole uint8

const (
	ReplicaRoleVoter ReplicaRole = iota
	ReplicaRoleAnonymous
)

// String returns a stable role name for status and logs.
func (role ReplicaRole) String() string {
	switch role {
	case ReplicaRoleVoter:
		return "voter"
	case ReplicaRoleAnonymous:
		return "anonymous"
	default:
		return "unknown"
	}
}

// ReplicaRoleRoster is a detached, node-sorted role view. Only Voters may be
// passed to quorum decisions; Anonymous members remain available for reads,
// bootstrap sources, and health reporting without changing quorum size.
type ReplicaRoleRoster struct {
	Generation uint64
	Voters     []string
	Anonymous  []string
}

// RoleRoster returns the current active membership split by quorum role.
// Pending joins and evicted tombstones are intentionally excluded.
func (admission *ReplicaJoinAdmission) RoleRoster() ReplicaRoleRoster {
	if admission == nil {
		return ReplicaRoleRoster{}
	}
	admission.mu.RLock()
	defer admission.mu.RUnlock()
	ids := make([]string, 0, len(admission.members))
	for nodeID := range admission.members {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	roster := ReplicaRoleRoster{
		Generation: admission.generation,
		Voters:     make([]string, 0, len(ids)),
		Anonymous:  make([]string, 0, len(ids)),
	}
	for _, nodeID := range ids {
		if admission.members[nodeID].Role == ReplicaRoleAnonymous {
			roster.Anonymous = append(roster.Anonymous, nodeID)
			continue
		}
		roster.Voters = append(roster.Voters, nodeID)
	}
	return roster
}

// ExecuteVoterWriteQuorum rejects anonymous and unknown targets before
// invoking the existing concurrent quorum executor. This keeps the role check
// on the same admission boundary as quorum execution rather than relying on a
// caller to remember to subtract anonymous replicas manually.
func ExecuteVoterWriteQuorum(ctx context.Context, roster ReplicaRoleRoster, nodes []string, required int, write WriteQuorumWriteFunc) (WriteQuorumResult, error) {
	if err := validateReplicaRoleRoster(roster); err != nil {
		return WriteQuorumResult{}, err
	}
	normalized, err := normalizeWriteQuorumNodes(nodes, required, write)
	if err != nil {
		return WriteQuorumResult{}, err
	}
	for _, nodeID := range normalized {
		if replicaRoleRosterContains(roster.Anonymous, nodeID) {
			return WriteQuorumResult{}, fmt.Errorf("%w: %s", ErrReplicaQuorumAnonymousTarget, nodeID)
		}
		if !replicaRoleRosterContains(roster.Voters, nodeID) {
			return WriteQuorumResult{}, fmt.Errorf("%w: %s", ErrReplicaQuorumUnknownTarget, nodeID)
		}
	}
	return executeWriteQuorumNormalized(ctx, normalized, required, write)
}

func validateReplicaRole(role ReplicaRole) error {
	if role > ReplicaRoleAnonymous {
		return fmt.Errorf("%w: %d", ErrReplicaRoleInvalid, role)
	}
	return nil
}

func validateReplicaRoleRoster(roster ReplicaRoleRoster) error {
	if err := validateReplicaRoleIDs(roster.Voters); err != nil {
		return err
	}
	if err := validateReplicaRoleIDs(roster.Anonymous); err != nil {
		return err
	}
	for _, nodeID := range roster.Anonymous {
		if replicaRoleRosterContains(roster.Voters, nodeID) {
			return fmt.Errorf("%w: node %s appears in both roles", ErrReplicaRoleRosterInvalid, nodeID)
		}
	}
	return nil
}

func validateReplicaRoleIDs(ids []string) error {
	previous := ""
	for index, nodeID := range ids {
		if _, err := normalizeReplicaJoinValue(nodeID, MaxReplicaJoinNodeIDBytes, "role roster node ID"); err != nil {
			return fmt.Errorf("%w: %v", ErrReplicaRoleRosterInvalid, err)
		}
		if index > 0 && nodeID <= previous {
			return fmt.Errorf("%w: role lists must be strictly sorted", ErrReplicaRoleRosterInvalid)
		}
		previous = nodeID
	}
	return nil
}

func replicaRoleRosterContains(sortedIDs []string, nodeID string) bool {
	index := sort.SearchStrings(sortedIDs, nodeID)
	return index < len(sortedIDs) && sortedIDs[index] == nodeID
}
