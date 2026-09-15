package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkBackupBundleRestorePartitionSelection(b *testing.B) {
	for _, format := range []SnapshotFormat{SnapshotFormatJSON, SnapshotFormatBinary} {
		b.Run(string(format), func(b *testing.B) {
			source := CreateHatTrie()
			defer source.Destroy()
			for index := 0; index < 4096; index++ {
				prefix := "region:us/"
				if index%2 == 0 {
					prefix = "region:sg/"
				}
				source.UpsertString(fmt.Sprintf("%suser:%04d", prefix, index), "value")
			}
			bundlePath := filepath.Join(b.TempDir(), "partitioned.tar.gz")
			_, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
				Mode:           BackupModeSnapshot,
				SnapshotFormat: format,
				Partition: BackupPartitionMetadata{
					Mode:        "partitioned",
					Local:       true,
					Partitions:  []string{"sg", "us"},
					KeyPrefixes: []string{"region:sg/", "region:us/"},
				},
				PartitionLocal: true,
			})
			if err != nil {
				b.Fatal(err)
			}

			for _, selection := range []struct {
				name   string
				option BackupBundleRestoreOptions
			}{
				{name: "full"},
				{
					name: "selected",
					option: BackupBundleRestoreOptions{Partition: &BackupPartitionMetadata{
						Mode:        "partitioned",
						Local:       true,
						Partitions:  []string{"sg"},
						KeyPrefixes: []string{"region:sg/"},
					}},
				},
			} {
				b.Run(selection.name, func(b *testing.B) {
					dataRoot := b.TempDir()
					b.ReportAllocs()
					b.ResetTimer()
					for index := 0; index < b.N; index++ {
						dataDir := filepath.Join(dataRoot, fmt.Sprintf("restore-%d", index))
						report, err := RestoreBackupBundle(bundlePath, dataDir, selection.option)
						if err != nil {
							b.Fatal(err)
						}
						if selection.name == "full" && report.RecoveredKeys != 4096 {
							b.Fatalf("full restore recovered %d keys, want 4096", report.RecoveredKeys)
						}
						if selection.name == "selected" && report.RecoveredKeys != 2048 {
							b.Fatalf("selected restore recovered %d keys, want 2048", report.RecoveredKeys)
						}
					}
					b.StopTimer()
					info, err := os.Stat(filepath.Join(dataRoot, fmt.Sprintf("restore-%d", b.N-1), backupBundleSnapshotPath))
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(info.Size()), "snapshot_bytes")
				})
			}
		})
	}
}
