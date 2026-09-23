package hatCache

import (
	"path/filepath"
	"testing"
)

func BenchmarkT213ScheduledSnapshotBaseline(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("benchmark", "scheduled-snapshot")

	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	snapshotPath := filepath.Join(b.TempDir(), "snapshot.hc")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := journal.SaveSnapshotWithFormat(trie, snapshotPath, SnapshotFormatBinary); err != nil {
			b.Fatalf("SaveSnapshotWithFormat() error = %v", err)
		}
	}
}
