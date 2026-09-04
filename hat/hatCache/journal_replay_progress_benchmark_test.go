package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkCommandJournalReplayProgress(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	for index := 0; index < 256; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("replay-benchmark-key-%d", index),
			Value:   "replay-benchmark-value",
		})
		if !response.OK {
			trie.Destroy()
			journal.Close()
			b.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}
	trie.Destroy()
	b.Cleanup(func() { _ = journal.Close() })

	for _, test := range []struct {
		name     string
		progress bool
	}{
		{name: "Legacy", progress: false},
		{name: "WithProgress", progress: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				replayed := CreateHatTrie()
				var err error
				if test.progress {
					_, err = journal.ReplayWithProgress(replayed, 0, 0)
				} else {
					_, err = journal.Replay(replayed, 0)
				}
				replayed.Destroy()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
