package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

var benchmarkCHU23AsyncCommandQueueStatsSink AsyncCommandQueueStats

func BenchmarkCHU23AsyncCommandQueueStats(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkCHU23AsyncCommandQueueStatsSink = journal.AsyncCommandQueueStats()
	}
}

func BenchmarkCHU23AsyncCommandQueueFlushEmpty(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := journal.FlushAsyncCommands(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
