package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkSegmentedCommandJournalRetention(b *testing.B) {
	for _, test := range []struct {
		name          string
		retainedBytes int64
	}{
		{name: "CountOnly", retainedBytes: 0},
		{name: "ByteBudget", retainedBytes: 1 << 30},
	} {
		b.Run(test.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "commands.journal")
			journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: 1,
				SegmentMaxBytes:     1,
				RetainedSegments:    MaxCommandJournalRetainedSegments,
				RetainedBytes:       test.retainedBytes,
			})
			if err != nil {
				b.Fatal(err)
			}
			trie := CreateHatTrie()
			for index := 0; index < 64; index++ {
				response := journal.ExecuteCommand(trie, CacheCommandRequest{
					Command: "SETSTR",
					Key:     fmt.Sprintf("benchmark-key-%d", index),
					Value:   "benchmark-value",
				})
				if !response.OK {
					trie.Destroy()
					journal.Close()
					b.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
				}
			}
			trie.Destroy()
			b.Cleanup(func() { _ = journal.Close() })
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				journal.mu.Lock()
				err := journal.pruneSegmentsLocked()
				journal.mu.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
