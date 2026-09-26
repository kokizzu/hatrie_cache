package hatTopology

import (
	"context"
	"testing"
	"time"
)

const partitionOwnershipConsensusBenchmarkDelay = 50 * time.Microsecond

func benchmarkPartitionOwnershipConsensusPolicy() (TopologyConsensusPolicy, PartitionOwnership) {
	return TopologyConsensusPolicy{
			Voters:   []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g"},
			Required: 7,
		}, PartitionOwnership{
			ShardID:             17,
			Primary:             "node-a",
			Replicas:            []string{"node-b", "node-c"},
			TopologyFingerprint: "topology-v17",
			FencingToken:        42,
		}
}

func benchmarkPartitionOwnershipConsensusVote(voter string, ownership PartitionOwnership) PartitionOwnershipConsensusVote {
	return PartitionOwnershipConsensusVote{
		NodeID:    voter,
		Ownership: ownership,
		Accepted:  true,
	}
}

func BenchmarkPartitionOwnershipConsensusCollectionBaseline(b *testing.B) {
	policy, expected := benchmarkPartitionOwnershipConsensusPolicy()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		votes := make([]PartitionOwnershipConsensusVote, 0, len(policy.Voters))
		for _, voter := range policy.Voters {
			time.Sleep(partitionOwnershipConsensusBenchmarkDelay)
			votes = append(votes, benchmarkPartitionOwnershipConsensusVote(voter, expected))
		}
		decision, err := EvaluatePartitionOwnershipConsensus(policy, expected, votes)
		if err != nil || !decision.Satisfied {
			b.Fatalf("serial baseline consensus failed: decision=%#v err=%v", decision, err)
		}
	}
}

func BenchmarkPartitionOwnershipConsensusCollection(b *testing.B) {
	policy, expected := benchmarkPartitionOwnershipConsensusPolicy()
	fetch := func(_ context.Context, voter string, ownership PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		time.Sleep(partitionOwnershipConsensusBenchmarkDelay)
		return benchmarkPartitionOwnershipConsensusVote(voter, ownership), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := CollectPartitionOwnershipConsensus(context.Background(), policy, expected, fetch, PartitionOwnershipConsensusCollectorOptions{MaxConcurrent: len(policy.Voters)})
		if err != nil || !result.Decision.Satisfied {
			b.Fatalf("collector consensus failed: result=%#v err=%v", result, err)
		}
	}
}
