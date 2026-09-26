package hatCache

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func c153gBenchmarkOwnership() hatTopology.PartitionOwnership {
	return hatTopology.PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "topology-v7",
		FencingToken:        19,
	}
}

func c153gBenchmarkAuthenticator(b testing.TB) *hatTopology.PartitionOwnershipConsensusAuthenticator {
	b.Helper()
	authenticator, err := hatTopology.NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		b.Fatal(err)
	}
	return authenticator
}

func BenchmarkC153gPartitionOwnershipConsensusDirect(b *testing.B) {
	authenticator := c153gBenchmarkAuthenticator(b)
	expected := c153gBenchmarkOwnership()
	vote, err := authenticator.Sign(hatTopology.PartitionOwnershipConsensusVote{NodeID: "node-b", Ownership: expected, Accepted: true})
	if err != nil {
		b.Fatal(err)
	}
	policy := hatTopology.TopologyConsensusPolicy{Voters: []string{"node-b"}, Required: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := hatTopology.CollectPartitionOwnershipConsensus(context.Background(), policy, expected, func(context.Context, string, hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			return vote, nil
		}, hatTopology.PartitionOwnershipConsensusCollectorOptions{Authenticator: authenticator})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC153gPartitionOwnershipConsensusGRPCFetch(b *testing.B) {
	authenticator := c153gBenchmarkAuthenticator(b)
	expected := c153gBenchmarkOwnership()
	conn, stop := newC153gOwnershipGRPCConnection(b, CacheGRPCOptions{
		NodeName:             "node-b",
		ReplicationAuthToken: "replication-secret",
		PartitionOwnershipConsensusVote: func(context.Context, hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			return authenticator.Sign(hatTopology.PartitionOwnershipConsensusVote{NodeID: "node-b", Ownership: expected, Accepted: true})
		},
		PartitionOwnershipConsensusAuthenticator: authenticator,
	})
	defer stop()
	client := NewPartitionOwnershipConsensusGRPCClient(conn, "replication-secret")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Fetch(context.Background(), expected); err != nil {
			b.Fatal(err)
		}
	}
}
