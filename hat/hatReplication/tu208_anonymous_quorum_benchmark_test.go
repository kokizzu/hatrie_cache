package hatReplication

import "testing"

func BenchmarkTU208AnonymousMemberWriteQuorumSetup(b *testing.B) {
	options := JournalWriteQuorumOptions{
		Enabled: true,
		Members: []QuorumMember{
			{Node: "node-a"},
			{Node: "node-b"},
			{Node: "node-c"},
			{Node: "node-anon", Anonymous: true},
		},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		quorum, err := NewJournalWriteQuorum(options)
		if err != nil || !quorum.Enabled() {
			b.Fatalf("NewJournalWriteQuorum() = %#v/%v", quorum, err)
		}
	}
}

func BenchmarkTU208AnonymousMemberWriteQuorumEvaluate(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled: true,
		Members: []QuorumMember{
			{Node: "node-a"},
			{Node: "node-b"},
			{Node: "node-c"},
			{Node: "node-anon", Anonymous: true},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "node-a", Sequence: 42, Accepted: true},
		{Node: "node-b", Sequence: 42, Accepted: true},
		{Node: "node-c", Sequence: 42, Accepted: true},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		decision, err := quorum.Evaluate(JournalWriteQuorumProposal{Sequence: 42}, acknowledgements)
		if err != nil || !decision.Satisfied {
			b.Fatalf("Evaluate() = %#v/%v", decision, err)
		}
	}
}

func BenchmarkTU208AnonymousMemberFilter(b *testing.B) {
	members := []QuorumMember{
		{Node: "node-b"},
		{Node: "node-anon", Anonymous: true},
		{Node: "node-a"},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		voters, err := QuorumVoterIDs(members)
		if err != nil || len(voters) != 2 {
			b.Fatalf("QuorumVoterIDs() = %#v/%v", voters, err)
		}
	}
}
