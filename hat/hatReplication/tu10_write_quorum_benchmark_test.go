package hatReplication

import (
	"context"
	"testing"
)

func BenchmarkTU10JournalWriteQuorumEvaluate(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"node-a", "node-b", "node-c"}, Required: 2})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 42, FenceToken: 7}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "node-a", Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true},
		{Node: "node-b", Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		decision, err := quorum.Evaluate(proposal, acknowledgements)
		if err != nil || !decision.Satisfied {
			b.Fatalf("Evaluate() = %#v/%v", decision, err)
		}
	}
}

func BenchmarkTU10JournalWriteQuorumDisabled(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if quorum.Enabled() {
			b.Fatal("disabled quorum is enabled")
		}
	}
}

func BenchmarkTU10JournalWriteQuorumExecute(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"node-a", "node-b", "node-c"}, Required: 2})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 42, FenceToken: 7}
	acknowledge := func(_ context.Context, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: node != "node-c"}, nil
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		decision, err := quorum.Execute(context.Background(), proposal, acknowledge)
		if err != nil || !decision.Satisfied {
			b.Fatalf("Execute() = %#v/%v", decision, err)
		}
	}
}
