package hatTopology

import "testing"

var partitionOwnershipConsensusBenchmarkSink TopologyConsensusDecision
var partitionOwnershipMetadataConsensusBenchmarkSink PartitionOwnershipConsensusDecision
var partitionOwnershipConsensusCollectorBenchmarkSink bool

func BenchmarkPartitionOwnershipConsensus(b *testing.B) {
	policy := TopologyConsensusPolicy{Voters: []string{"node-a", "node-b", "node-c", "node-d"}, Required: 3}
	votes := []TopologyConsensusVote{
		{NodeID: "node-d", Accepted: true, ExpectedFingerprint: "old-topology", CandidateFingerprint: "new-topology"},
		{NodeID: "node-b", Accepted: true, ExpectedFingerprint: "old-topology", CandidateFingerprint: "new-topology"},
		{NodeID: "node-a", Accepted: true, ExpectedFingerprint: "old-topology", CandidateFingerprint: "new-topology"},
		{NodeID: "node-c", Accepted: false, ExpectedFingerprint: "old-topology", CandidateFingerprint: "wrong-topology"},
	}
	b.Run("fingerprint_only", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			decision, err := EvaluateTopologyConsensus(policy, "old-topology", "new-topology", votes)
			if err != nil {
				b.Fatal(err)
			}
			partitionOwnershipConsensusBenchmarkSink = decision
		}
	})

	expected := PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "new-topology",
		FencingToken:        11,
	}
	metadataVotes := []PartitionOwnershipConsensusVote{
		{NodeID: "node-d", Accepted: true, Ownership: expected},
		{NodeID: "node-b", Accepted: true, Ownership: expected},
		{NodeID: "node-a", Accepted: true, Ownership: expected},
		{NodeID: "node-c", Accepted: false, Ownership: PartitionOwnership{
			ShardID:             expected.ShardID,
			Primary:             "node-old",
			Replicas:            append([]string(nil), expected.Replicas...),
			TopologyFingerprint: expected.TopologyFingerprint,
			FencingToken:        expected.FencingToken,
		}},
	}
	b.Run("ownership_metadata", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			decision, err := EvaluatePartitionOwnershipConsensus(policy, expected, metadataVotes)
			if err != nil {
				b.Fatal(err)
			}
			partitionOwnershipMetadataConsensusBenchmarkSink = decision
		}
	})

	b.Run("ownership_collector_vote_loop", func(b *testing.B) {
		collector, err := NewPartitionOwnershipConsensusCollector(policy, expected)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := collector.Reset(); err != nil {
				b.Fatal(err)
			}
			terminal := false
			for _, vote := range metadataVotes {
				terminal, err = collector.AddVote(vote)
				if err != nil {
					b.Fatal(err)
				}
				if terminal {
					break
				}
			}
			partitionOwnershipConsensusCollectorBenchmarkSink = terminal
		}
	})

	b.Run("ownership_collector_round", func(b *testing.B) {
		collector, err := NewPartitionOwnershipConsensusCollector(policy, expected)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		collectorVotes := []PartitionOwnershipConsensusVote{metadataVotes[3], metadataVotes[0], metadataVotes[1], metadataVotes[2]}
		for range b.N {
			if err := collector.Reset(); err != nil {
				b.Fatal(err)
			}
			for _, vote := range collectorVotes {
				if _, err := collector.AddVote(vote); err != nil {
					b.Fatal(err)
				}
			}
			decision, err := collector.Decision()
			if err != nil {
				b.Fatal(err)
			}
			partitionOwnershipMetadataConsensusBenchmarkSink = decision
		}
	})

	b.Run("ownership_collector_fresh_round", func(b *testing.B) {
		collectorVotes := []PartitionOwnershipConsensusVote{metadataVotes[3], metadataVotes[0], metadataVotes[1], metadataVotes[2]}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			collector, err := NewPartitionOwnershipConsensusCollector(policy, expected)
			if err != nil {
				b.Fatal(err)
			}
			for _, vote := range collectorVotes {
				if _, err := collector.AddVote(vote); err != nil {
					b.Fatal(err)
				}
			}
			decision, err := collector.Decision()
			if err != nil {
				b.Fatal(err)
			}
			partitionOwnershipMetadataConsensusBenchmarkSink = decision
		}
	})
}
