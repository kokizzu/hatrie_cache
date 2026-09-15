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
			journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
			if err != nil {
				b.Fatal(err)
			}
			if response := journal.ExecuteCommand(source, CacheCommandRequest{
				Command: "SETSTR",
				Key:     "region:sg/journal-marker",
				Value:   "value",
			}); !response.OK {
				b.Fatalf("ExecuteCommand() = %#v, want ok", response)
			}
			checkpointJournalBundlePath := filepath.Join(b.TempDir(), "partitioned-checkpoint-journal.tar.gz")
			_, err = CreateBackupBundle(checkpointJournalBundlePath, source, journal, BackupBundleOptions{
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
				_ = journal.Close()
				b.Fatal(err)
			}
			for index := 0; index < 128; index++ {
				prefix := "region:us/"
				if index%2 == 0 {
					prefix = "region:sg/"
				}
				if response := journal.ExecuteCommand(source, CacheCommandRequest{
					Command: "SETSTR",
					Key:     fmt.Sprintf("%sreplay:%03d", prefix, index),
					Value:   "tail",
				}); !response.OK {
					b.Fatalf("tail ExecuteCommand() = %#v, want ok", response)
				}
			}
			journalPath := journal.path
			if err := journal.Close(); err != nil {
				b.Fatal(err)
			}
			replayTailBundlePath := rebuildBackupBundleWithJournal(b, checkpointJournalBundlePath, journalPath)

			selectedOption := BackupBundleRestoreOptions{Partition: &BackupPartitionMetadata{
				Mode:        "partitioned",
				Local:       true,
				Partitions:  []string{"sg"},
				KeyPrefixes: []string{"region:sg/"},
			}}

			for _, selection := range []struct {
				name          string
				bundle        string
				option        BackupBundleRestoreOptions
				wantRecovered int
			}{
				{name: "full", bundle: bundlePath, wantRecovered: 4096},
				{
					name:          "selected",
					bundle:        bundlePath,
					option:        selectedOption,
					wantRecovered: 2048,
				},
				{
					name:          "selected-checkpoint-journal",
					bundle:        checkpointJournalBundlePath,
					option:        selectedOption,
					wantRecovered: 2049,
				},
				{
					name:          "selected-replay-tail",
					bundle:        replayTailBundlePath,
					option:        selectedOption,
					wantRecovered: 2113,
				},
			} {
				b.Run(selection.name, func(b *testing.B) {
					dataRoot := b.TempDir()
					b.ReportAllocs()
					b.ResetTimer()
					for index := 0; index < b.N; index++ {
						dataDir := filepath.Join(dataRoot, fmt.Sprintf("restore-%d", index))
						report, err := RestoreBackupBundle(selection.bundle, dataDir, selection.option)
						if err != nil {
							b.Fatal(err)
						}
						if report.RecoveredKeys != selection.wantRecovered {
							b.Fatalf("%s restore recovered %d keys, want %d", selection.name, report.RecoveredKeys, selection.wantRecovered)
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
