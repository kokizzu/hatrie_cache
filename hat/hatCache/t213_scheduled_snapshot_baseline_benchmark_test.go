//go:build t213baseline

package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkT213ScheduledSnapshotBaseline(b *testing.B) {
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
	destination := filepath.Join(b.TempDir(), "latest.snapshot")

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := journal.WriteSnapshotWithResumableExport(trie, destination, SnapshotExportOptions{
			Format: SnapshotFormatBinary,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
