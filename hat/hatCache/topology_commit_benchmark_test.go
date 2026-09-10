package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func benchmarkTopology(version, fencingToken uint64) ClusterTopology {
	return ClusterTopology{
		Version:      version,
		Mode:         TopologyModeFullReplica,
		FencingToken: fencingToken,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
		},
	}
}

func BenchmarkTopologyStoreSet(b *testing.B) {
	store, err := NewTopologyStore(benchmarkTopology(1, 1))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Set(benchmarkTopology(1, uint64(i+2))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTopologyStoreApplyCommit(b *testing.B) {
	store, err := NewTopologyStore(benchmarkTopology(1, 1))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expected := store.Fingerprint()
		commit, err := hatTopology.NewTopologyCommit(expected, benchmarkTopology(1, uint64(i+2)))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := store.ApplyCommit(commit); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTopologyConsensus(b *testing.B) {
	policy := hatTopology.TopologyConsensusPolicy{Voters: []string{"node-a", "node-b", "node-c"}}
	votes := []hatTopology.TopologyConsensusVote{
		{NodeID: "node-a", ExpectedFingerprint: "current", CandidateFingerprint: "candidate", Accepted: true},
		{NodeID: "node-b", ExpectedFingerprint: "current", CandidateFingerprint: "candidate", Accepted: true},
		{NodeID: "node-c", ExpectedFingerprint: "current", CandidateFingerprint: "candidate", Accepted: false},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hatTopology.EvaluateTopologyConsensus(policy, "current", "candidate", votes); err != nil {
			b.Fatal(err)
		}
	}
}
