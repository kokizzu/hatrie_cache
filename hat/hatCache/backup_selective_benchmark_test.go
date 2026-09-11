package hatCache

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkBackupBundleSelectiveBaseline(b *testing.B) {
	for _, format := range []SnapshotFormat{SnapshotFormatJSON, SnapshotFormatBinary} {
		b.Run(string(format), func(b *testing.B) {
			for _, selection := range []struct {
				name     string
				prefixes []string
			}{
				{name: "full"},
				{name: "selected", prefixes: []string{"keep:"}},
			} {
				b.Run(selection.name, func(b *testing.B) {
					trie := CreateHatTrie()
					defer trie.Destroy()
					for index := 0; index < 1000; index++ {
						value := strconv.Itoa(index)
						trie.UpsertString("keep:"+value, "kept-value-"+value)
						trie.UpsertString("drop:"+value, "excluded-value-"+value)
					}
					path := filepath.Join(b.TempDir(), "snapshot.tar.gz")
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if _, err := CreateBackupBundle(path, trie, nil, BackupBundleOptions{
							Mode:           BackupModeSnapshot,
							SnapshotFormat: format,
							KeyPrefixes:    selection.prefixes,
						}); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if info, err := os.Stat(path); err == nil {
						b.ReportMetric(float64(info.Size()), "bundle_bytes")
					}
				})
			}
		})
	}
}
