package hatReplication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTU047ClusterWriteCommitWithLedger(b *testing.B) {
	callback := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		path := filepath.Join(b.TempDir(), fmt.Sprintf("ledger-%d.bin", index))
		ledger, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{MaxEntries: 1})
		if err != nil {
			b.Fatalf("OpenClusterWriteCommitLedger() error = %v", err)
		}
		proposal := ClusterWriteCommitProposal{TransactionID: fmt.Sprintf("benchmark-%d", index), FenceToken: 1}
		b.StartTimer()
		result, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a", "node-b", "node-c"}, proposal, callback, callback, callback, ledger)
		b.StopTimer()
		if err != nil || !result.Committed {
			b.Fatalf("ExecuteClusterWriteCommitWithLedger() = %#v/%v", result, err)
		}
		if err := ledger.Close(); err != nil {
			b.Fatalf("ledger.Close() error = %v", err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatalf("remove benchmark ledger: %v", err)
		}
		b.StartTimer()
	}
}
