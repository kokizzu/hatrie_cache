package hatReplication

import "testing"

var t203WriteSink uint64

func t203UnfencedWrite() error {
	t203WriteSink++
	return nil
}

func BenchmarkT203UnfencedWritePath(b *testing.B) {
	for index := 0; index < b.N; index++ {
		if err := t203UnfencedWrite(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT203ExistingJournalQuorumEvaluate(b *testing.B) {
	quorum, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
		Enabled:  true,
		Voters:   []string{"node-a", "node-b", "node-c"},
		Required: 2,
	})
	if err != nil {
		b.Fatal(err)
	}
	proposal := JournalWriteQuorumProposal{Sequence: 42, FenceToken: 7}
	acknowledgements := []JournalWriteQuorumAcknowledgement{
		{Node: "node-a", Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true},
		{Node: "node-b", Sequence: proposal.Sequence, FenceToken: proposal.FenceToken, Accepted: true},
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision, err := quorum.Evaluate(proposal, acknowledgements)
		if err != nil {
			b.Fatal(err)
		}
		t203WriteSink = uint64(decision.Acknowledged)
	}
}

func BenchmarkT203FencedWritePath(b *testing.B) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	write := ReplicaSetLeaderWrite{NodeID: "node-a", Term: 1, FencingToken: 1}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := fence.Execute(write, t203UnfencedWrite); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT203StaleWriteRejection(b *testing.B) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	write := ReplicaSetLeaderWrite{NodeID: "node-old", Term: 1, FencingToken: 1}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := fence.Execute(write, t203UnfencedWrite); err == nil {
			b.Fatal("stale write was admitted")
		}
	}
}

func BenchmarkT203LeaderAdvance(b *testing.B) {
	fence, err := NewReplicaSetLeaderWriteFence(ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value := uint64(index) + 2
		if _, err := fence.Advance(ReplicaSetLeaderWriteFenceTransition{
			LeaderID:     "node-b",
			Term:         value,
			FencingToken: value,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
