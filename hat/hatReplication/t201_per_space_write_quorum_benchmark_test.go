package hatReplication

import (
	"context"
	"testing"
)

var t201JournalAcknowledge JournalWriteQuorumAcknowledgeFunc = func(_ context.Context, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
	return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true}, nil
}

var t201PerSpaceAcknowledge PerSpaceWriteQuorumAcknowledgeFunc = func(_ context.Context, _, node string, proposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
	return JournalWriteQuorumAcknowledgement{Node: node, Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true}, nil
}

func BenchmarkT201DirectJournalWriteQuorumExecute(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{Enabled: true, Voters: []string{"local", "east", "west"}, Required: 2})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 4}
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		if _, err := quorum.Execute(ctx, proposal, t201JournalAcknowledge); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT201PerSpaceWriteQuorumExecute(b *testing.B) {
	quorum, err := NewPerSpaceWriteQuorum(PerSpaceWriteQuorumOptions{Policies: []PerSpaceWriteQuorumPolicy{{
		Space: "critical_orders", Voters: []string{"local", "east", "west"}, Required: 2,
	}}})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17, FenceToken: 4}
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		if _, err := quorum.Execute(ctx, "critical_orders", proposal, t201PerSpaceAcknowledge); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT201PerSpaceWriteQuorumUnconfiguredLookup(b *testing.B) {
	quorum, err := NewPerSpaceWriteQuorum(PerSpaceWriteQuorumOptions{})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 17}
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		result, err := quorum.Execute(ctx, "ordinary_events", proposal, nil)
		if err != nil || result.Enforced {
			b.Fatalf("unconfigured result/error = %+v/%v", result, err)
		}
	}
}
