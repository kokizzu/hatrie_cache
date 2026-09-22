//go:build t213

package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkT213ScheduledSnapshot(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < 128; index++ {
		response := trie.ExecuteCommand(CacheCommandRequest{
			Command: "SET",
			Key:     fmt.Sprintf("scheduled-%03d", index),
			Value:   "snapshot-value",
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand() response = %#v", response)
		}
	}

	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	scheduler, err := NewScheduledSnapshotter(journal, trie, ScheduledSnapshotOptions{
		Interval:    time.Hour,
		Destination: filepath.Join(b.TempDir(), "latest.snapshot"),
		Format:      SnapshotFormatBinary,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := scheduler.RunNow(); err != nil {
			b.Fatal(err)
		}
	}
}
