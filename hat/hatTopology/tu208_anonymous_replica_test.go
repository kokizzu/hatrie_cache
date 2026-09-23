package hatTopology

import (
	"errors"
	"reflect"
	"testing"
)

func TestTU208AnonymousReplicaIsAcceptedButExcludedFromQuorumVoters(t *testing.T) {
	topology := ClusterTopology{
		Version: Version,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-b", Address: "b:8000", Role: TopologyRoleReplica},
			{ID: "node-anon", Address: "anon:8000", Role: TopologyRoleAnonymous},
			{ID: "node-a", Address: "a:8000", Role: TopologyRolePrimary},
		},
	}
	normalized, err := Normalize(topology)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	voters, err := QuorumVoterIDs(normalized)
	if err != nil {
		t.Fatalf("QuorumVoterIDs() error = %v", err)
	}
	if want := []string{"node-a", "node-b"}; !reflect.DeepEqual(voters, want) {
		t.Fatalf("quorum voters = %#v, want %#v", voters, want)
	}
}

func TestTU208QuorumVoterIDsRejectsInvalidMembers(t *testing.T) {
	_, err := QuorumVoterIDs(ClusterTopology{Nodes: []TopologyNode{{ID: "node-a"}, {ID: "node-a"}}})
	if !errors.Is(err, ErrQuorumVoterIDsInvalid) {
		t.Fatalf("duplicate topology voters error = %v, want ErrQuorumVoterIDsInvalid", err)
	}
	_, err = QuorumVoterIDs(ClusterTopology{Nodes: []TopologyNode{{ID: "node-anon", Role: TopologyRoleAnonymous}}})
	if !errors.Is(err, ErrQuorumVoterIDsInvalid) {
		t.Fatalf("all anonymous topology voters error = %v, want ErrQuorumVoterIDsInvalid", err)
	}
}

func TestTU208AnonymousReplicaCanJoinMembershipButCannotOwnShard(t *testing.T) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{})
	if err != nil {
		t.Fatalf("NewMembershipJournal() error = %v", err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-anonymous",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-anon", Address: "anon:8000", Role: TopologyRoleAnonymous},
	}); err != nil {
		t.Fatalf("anonymous membership join error = %v", err)
	}

	_, err = Normalize(ClusterTopology{
		Version: Version,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-anon", Address: "anon:8000", Role: TopologyRoleAnonymous},
		},
		Shards: []TopologyShard{{ID: 1, Primary: "node-anon"}},
	})
	if err == nil {
		t.Fatal("Normalize() accepted anonymous node as shard primary")
	}
}
