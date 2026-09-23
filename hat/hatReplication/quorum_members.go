package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrQuorumMembersInvalid reports malformed quorum membership metadata.
	ErrQuorumMembersInvalid = errors.New("hatriecache: quorum members are invalid")
)

// QuorumMember describes one node's participation in quorum decisions.
// Anonymous members may receive replication, but never count as voters.
type QuorumMember struct {
	Node      string `json:"node"`
	Anonymous bool   `json:"anonymous,omitempty"`
}

// QuorumVoterIDs returns sorted, unique IDs for non-anonymous members. It is
// suitable for ExecuteReadQuorum and other count-based quorum APIs.
func QuorumVoterIDs(members []QuorumMember) ([]string, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("%w: members are empty", ErrQuorumMembersInvalid)
	}
	voterCount := 0
	for _, member := range members {
		if !member.Anonymous {
			voterCount++
		}
	}
	voters := make([]string, 0, voterCount)
	for index, member := range members {
		node := strings.TrimSpace(member.Node)
		if node == "" {
			return nil, fmt.Errorf("%w: node ID is empty", ErrQuorumMembersInvalid)
		}
		for previous := 0; previous < index; previous++ {
			if strings.TrimSpace(members[previous].Node) == node {
				return nil, fmt.Errorf("%w: duplicate node %q", ErrQuorumMembersInvalid, node)
			}
		}
		if !member.Anonymous {
			voters = append(voters, node)
		}
	}
	if len(voters) == 0 {
		return nil, fmt.Errorf("%w: members contain no voters", ErrQuorumMembersInvalid)
	}
	sort.Strings(voters)
	return voters, nil
}
