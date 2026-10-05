package hatTopology

import "testing"

var (
	tu12FailoverProposalBenchmarkSink FailoverProposal
	tu12ManualFailoverBenchmarkSink   TopologyCommit
)

func BenchmarkElectionStoreFailoverProposal(b *testing.B) {
	store := newTU12FailoverBenchmarkStore(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proposal, err := store.ProposeFailover(1, FailoverOptions{})
		if err != nil {
			b.Fatal(err)
		}
		tu12FailoverProposalBenchmarkSink = proposal
	}
}

func BenchmarkElectionStoreFailoverManualProposal(b *testing.B) {
	store := newTU12FailoverBenchmarkStore(b)
	provider := store.topology.(tu12TopologyProvider)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		commit, err := tu12ManualFailoverProposal(store, provider.topology)
		if err != nil {
			b.Fatal(err)
		}
		tu12ManualFailoverBenchmarkSink = commit
	}
}

func newTU12FailoverBenchmarkStore(b *testing.B) *ElectionStore {
	b.Helper()
	provider := tu12TopologyProvider{topology: ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		Self:         "node-a",
		FencingToken: 7,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
			{ID: "node-c"},
		},
		Shards: []TopologyShard{{ID: 1, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}},
	}}
	store := NewElectionStore(provider, ElectionOptions{})
	if err := store.MarkOffline("node-a"); err != nil {
		b.Fatal(err)
	}
	return store
}

func tu12ManualFailoverProposal(store *ElectionStore, topology ClusterTopology) (TopologyCommit, error) {
	normalized, err := Normalize(topology)
	if err != nil {
		return TopologyCommit{}, err
	}
	shard := normalized.Shards[0]
	active := store.ActiveNodes(normalized)
	candidate := ""
	for _, owner := range Owners(shard) {
		if owner != shard.Primary && active[owner] {
			candidate = owner
			break
		}
	}
	candidateTopology := Clone(normalized)
	candidateTopology.FencingToken++
	candidateTopology.Shards[0].Primary = candidate
	candidateTopology.Shards[0].Replicas = []string{shard.Primary, "node-c"}
	if candidate == "node-c" {
		candidateTopology.Shards[0].Replicas = []string{shard.Primary, "node-b"}
	}
	candidateTopology, err = Normalize(candidateTopology)
	if err != nil {
		return TopologyCommit{}, err
	}
	return NewTopologyCommit(normalized.Fingerprint(), candidateTopology)
}
