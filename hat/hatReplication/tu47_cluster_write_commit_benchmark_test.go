package hatReplication

import (
	"context"
	"testing"
)

func BenchmarkTU047ClusterWriteCommit(b *testing.B) {
	nodes := []string{"node-a", "node-b", "node-c"}
	proposal := ClusterWriteCommitProposal{TransactionID: "benchmark", FenceToken: 1}
	callback := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proposal.Sequence = uint64(index + 1)
		result, err := ExecuteClusterWriteCommit(context.Background(), nodes, proposal, callback, callback, callback)
		if err != nil || !result.Committed {
			b.Fatalf("result = %#v/%v", result, err)
		}
	}
}

func BenchmarkTU047ExistingWriteQuorum(b *testing.B) {
	nodes := []string{"node-a", "node-b", "node-c"}
	callback := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorum(context.Background(), nodes, len(nodes), callback)
		if err != nil || !result.Decision.Satisfied {
			b.Fatalf("result = %#v/%v", result, err)
		}
	}
}
