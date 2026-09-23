package hatReplication

import (
	"errors"
	"reflect"
	"testing"
)

func TestTU208JournalWriteQuorumExcludesAnonymousMembers(t *testing.T) {
	members := []QuorumMember{
		{Node: "node-b"},
		{Node: "node-anon", Anonymous: true},
		{Node: "node-a"},
	}
	voters, err := QuorumVoterIDs(members)
	if err != nil {
		t.Fatalf("QuorumVoterIDs() error = %v", err)
	}
	if want := []string{"node-a", "node-b"}; !reflect.DeepEqual(voters, want) {
		t.Fatalf("quorum voters = %#v, want %#v", voters, want)
	}

	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Members: members})
	if err != nil {
		t.Fatalf("NewJournalWriteQuorum() error = %v", err)
	}
	decision, err := quorum.Evaluate(JournalWriteQuorumProposal{Sequence: 7}, []JournalWriteQuorumAcknowledgement{
		{Node: "node-a", Sequence: 7, Accepted: true},
		{Node: "node-b", Sequence: 7, Accepted: true},
	})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if decision.Total != 2 || decision.Required != 2 || !decision.Satisfied {
		t.Fatalf("decision = %#v, want two voters and a satisfied majority", decision)
	}

	_, err = quorum.Evaluate(JournalWriteQuorumProposal{Sequence: 7}, []JournalWriteQuorumAcknowledgement{
		{Node: "node-anon", Sequence: 7, Accepted: true},
	})
	if !errors.Is(err, ErrJournalWriteQuorumInvalidAcknowledgement) {
		t.Fatalf("anonymous acknowledgement error = %v, want invalid acknowledgement", err)
	}
}

func TestTU208QuorumVoterIDsRejectsEmptyOrAllAnonymousMembers(t *testing.T) {
	_, err := QuorumVoterIDs([]QuorumMember{{Node: " "}})
	if !errors.Is(err, ErrQuorumMembersInvalid) {
		t.Fatalf("empty member error = %v, want ErrQuorumMembersInvalid", err)
	}
	_, err = QuorumVoterIDs([]QuorumMember{{Node: "node-anon", Anonymous: true}})
	if !errors.Is(err, ErrQuorumMembersInvalid) {
		t.Fatalf("all anonymous member error = %v, want ErrQuorumMembersInvalid", err)
	}
	_, err = NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled: true,
		Voters:  []string{"node-a"},
		Members: []QuorumMember{{Node: "node-a"}},
	})
	if !errors.Is(err, ErrJournalWriteQuorumInvalidOptions) {
		t.Fatalf("mixed voter configuration error = %v, want invalid options", err)
	}
}
