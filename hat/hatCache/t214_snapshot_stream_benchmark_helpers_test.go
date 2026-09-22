//go:build t214 || t214baseline

package hatCache

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
)

func t214BenchmarkSnapshot(b *testing.B) ([]byte, [sha256.Size]byte) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < 128; index++ {
		response := trie.ExecuteCommand(CacheCommandRequest{
			Command: "SET",
			Key:     fmt.Sprintf("stream-%03d", index),
			Value:   "snapshot-value",
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand() response = %#v", response)
		}
	}
	var snapshot bytes.Buffer
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	if _, err := journal.WriteSnapshotWithFormat(trie, &snapshot, SnapshotFormatGzipBinary); err != nil {
		b.Fatal(err)
	}
	return snapshot.Bytes(), sha256.Sum256(snapshot.Bytes())
}
