package hatCache

import (
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

// BenchmarkT211JournalSyncModes measures the journal path with the same
// no-op sync hook so the benchmark isolates mode dispatch and append costs.
func BenchmarkT211JournalSyncModes(b *testing.B) {
	for _, test := range []struct {
		name string
		mode hatJournal.SyncMode
	}{
		{name: "Periodic", mode: hatJournal.SyncModePeriodic},
		{name: "Immediate", mode: hatJournal.SyncModeImmediate},
		{name: "Disabled", mode: hatJournal.SyncModeDisabled},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				SyncMode:            test.mode,
				GroupCommitMaxBatch: 64,
			})
			if err != nil {
				b.Fatal(err)
			}
			trie := CreateHatTrie()
			b.Cleanup(func() {
				_ = journal.Close()
				trie.Destroy()
			})
			var syncs int
			journal.syncHook = func() error {
				syncs++
				return nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				response := journal.ExecuteCommand(trie, CacheCommandRequest{
					Command: "SETSTR",
					Key:     "t211-benchmark",
					Value:   "value",
				})
				if !response.OK {
					b.Fatal(response.Message)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncs)/float64(b.N), "syncs/op")
		})
	}
}
