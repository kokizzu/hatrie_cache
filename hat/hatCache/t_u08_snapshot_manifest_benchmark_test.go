package hatCache

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

func BenchmarkCommandJournalWriteSnapshot(b *testing.B) {
	b.Run("existing", func(b *testing.B) {
		benchmarkCommandJournalWriteSnapshot(b, false)
	})
	b.Run("manifest", func(b *testing.B) {
		benchmarkCommandJournalWriteSnapshot(b, true)
	})
}

func benchmarkCommandJournalWriteSnapshot(b *testing.B, manifest bool) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < 64; index++ {
		response := trie.ExecuteCommand(CacheCommandRequest{
			Command: "SET",
			Key:     fmt.Sprintf("key-%02d", index),
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

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if manifest {
			if _, err := journal.WriteSnapshotWithManifest(trie, io.Discard, SnapshotFormatBinary); err != nil {
				b.Fatal(err)
			}
			continue
		}
		if _, err := journal.WriteSnapshotWithFormat(trie, io.Discard, SnapshotFormatBinary); err != nil {
			b.Fatal(err)
		}
	}
}
