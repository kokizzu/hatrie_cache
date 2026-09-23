package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkT211JournalSyncBaseline(b *testing.B) {
	for _, test := range []struct {
		name      string
		maxBatch  int
		groupSync bool
	}{
		{name: "ImmediateBatch1", maxBatch: 1},
		{name: "PeriodicBatch64", maxBatch: 64, groupSync: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "commands.journal")
			journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: test.maxBatch,
			})
			if err != nil {
				b.Fatal(err)
			}
			trie := CreateHatTrie()
			b.Cleanup(func() {
				trie.Destroy()
				_ = journal.Close()
			})
			syncCalls := 0
			journal.syncHook = func() error {
				syncCalls++
				return nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				response := journal.ExecuteCommand(trie, CacheCommandRequest{
					Command: "SETSTR",
					Key:     fmt.Sprintf("t211:%d", index),
					Value:   "value",
				})
				if !response.OK {
					b.Fatal(response.Message)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncCalls)/float64(b.N), "syncs/op")
			if test.groupSync && syncCalls == 0 && b.N > 0 {
				b.Fatal("group commit baseline did not invoke sync")
			}
		})
	}
}
