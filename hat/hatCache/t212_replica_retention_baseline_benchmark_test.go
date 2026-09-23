package hatCache

import (
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkT212ReplicaRetentionBaseline(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     256,
		RetainedSegments:    1,
	})
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	b.Cleanup(func() {
		_ = journal.Close()
		trie.Destroy()
	})
	value := strings.Repeat("value", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "t212-benchmark",
			Value:   value,
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
	b.StopTimer()
	segments, err := listCommandJournalSegments(journal.path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(segments)), "segments")
}
