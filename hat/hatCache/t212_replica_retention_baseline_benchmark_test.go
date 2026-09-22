//go:build t212baseline

package hatCache

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkT212DefaultSegmentPruneBaseline(b *testing.B) {
	path := filepath.Join(b.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     256,
		RetainedSegments:    1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < 32; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("t212-baseline:%d", index),
			Value:   strings.Repeat("value", 64),
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		journal.mu.Lock()
		err = journal.pruneSegmentsLocked()
		journal.mu.Unlock()
		if err != nil {
			b.Fatal(err)
		}
	}
}
