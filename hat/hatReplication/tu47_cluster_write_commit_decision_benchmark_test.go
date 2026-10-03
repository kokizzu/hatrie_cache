package hatReplication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTU047DurableClusterWriteCommit(b *testing.B) {
	directory := b.TempDir()
	path := filepath.Join(directory, "decisions.state")
	nodes := []string{"node-a", "node-b", "node-c"}
	prepare := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	commit := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	abort := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			b.Fatal(err)
		}
		store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
		if err != nil {
			b.Fatal(err)
		}
		proposal := ClusterWriteCommitProposal{TransactionID: fmt.Sprintf("tx-%d", index), Sequence: uint64(index + 1), FenceToken: 7}
		b.StartTimer()
		result, err := ExecuteClusterWriteCommitDurable(context.Background(), nodes, proposal, prepare, commit, abort, store)
		b.StopTimer()
		if err != nil || !result.Committed {
			b.Fatalf("durable commit = %#v/%v", result, err)
		}
		b.StartTimer()
	}
}
