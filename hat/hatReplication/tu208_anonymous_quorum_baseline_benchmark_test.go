package hatReplication

import "testing"

func BenchmarkTU208ExistingExplicitWriteQuorumSetup(b *testing.B) {
	options := JournalWriteQuorumOptions{
		Enabled: true,
		Voters:  []string{"node-a", "node-b", "node-c"},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		quorum, err := NewJournalWriteQuorum(options)
		if err != nil || !quorum.Enabled() {
			b.Fatalf("NewJournalWriteQuorum() = %#v/%v", quorum, err)
		}
	}
}

func BenchmarkTU208ExistingExplicitWriteQuorumEvaluate(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled: true,
		Voters:  []string{"node-a", "node-b", "node-c"},
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
