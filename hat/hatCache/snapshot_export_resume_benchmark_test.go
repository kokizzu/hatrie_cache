package hatCache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkSnapshotExport(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journalDir := b.TempDir()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(journalDir, "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	for index := 0; index < 256; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SET",
			Key:     fmt.Sprintf("benchmark:%04d", index),
			Value:   fmt.Sprintf("value-%04d", index),
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand() response = %#v", response)
		}
	}
	targetPath := filepath.Join(b.TempDir(), "snapshot.hc")
	samplePath := filepath.Join(b.TempDir(), "sample.hc")
	if err := trie.SaveSnapshotWithJournalSequenceAndFormat(samplePath, journal.Sequence(), SnapshotFormatBinary); err != nil {
		b.Fatal(err)
	}
	sampleInfo, err := fileInfoForSnapshotExportBenchmark(samplePath)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(sampleInfo)
	b.ReportMetric(float64(sampleInfo), "snapshot-bytes")

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(sampleInfo)
		b.ReportMetric(float64(sampleInfo), "payload-io-B")
		for iteration := 0; iteration < b.N; iteration++ {
			if err := trie.SaveSnapshotWithJournalSequenceAndFormat(targetPath, journal.Sequence(), SnapshotFormatBinary); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("resumable", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(sampleInfo)
		b.ReportMetric(float64(sampleInfo*2), "payload-io-B")
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := journal.WriteSnapshotWithResumableExport(trie, targetPath, SnapshotExportOptions{
				Format: SnapshotFormatBinary,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func fileInfoForSnapshotExportBenchmark(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return 0, statErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if info.Size() == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return info.Size(), nil
}
