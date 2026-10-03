package hatCache

import (
	"testing"
	"time"
)

func benchmarkT018CommandJournalWrite(b *testing.B, options CommandJournalOptions, noOpSync bool) {
	journal, err := OpenCommandJournalWithOptions(b.TempDir()+"/commands.journal", options)
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	b.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Error(err)
		}
	})
	if noOpSync {
		journal.syncHook = func() error { return nil }
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "benchmark:key",
			Value:   "value",
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand(%d) failed: %s", index, response.Message)
		}
	}
}

func BenchmarkT018CommandJournalDefaultWrite(b *testing.B) {
	benchmarkT018CommandJournalWrite(b, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
	}, false)
}

func BenchmarkT018CommandJournalPeriodicWrite(b *testing.B) {
	benchmarkT018CommandJournalWrite(b, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		SyncMode:            CommandJournalSyncModePeriodic,
		SyncInterval:        time.Hour,
	}, false)
}

func BenchmarkT018CommandJournalDisabledWrite(b *testing.B) {
	benchmarkT018CommandJournalWrite(b, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		SyncMode:            CommandJournalSyncModeNone,
	}, false)
}

func BenchmarkT018CommandJournalDefaultWriteNoopSync(b *testing.B) {
	benchmarkT018CommandJournalWrite(b, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
	}, true)
}
