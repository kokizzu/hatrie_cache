package hatCache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkT213ScheduledSnapshotModes(b *testing.B) {
	for _, mode := range []string{"ExistingSaveSnapshot", "ScheduledWithManifest"} {
		b.Run(mode, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			trie.UpsertString("benchmark", "scheduled-snapshot")

			journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
			if err != nil {
				b.Fatalf("OpenCommandJournal() error = %v", err)
			}
			defer journal.Close()
			snapshotPath := filepath.Join(b.TempDir(), "snapshot.hc")

			var scheduler *ScheduledSnapshotScheduler
			if mode == "ScheduledWithManifest" {
				scheduler, err = StartScheduledSnapshots(context.Background(), journal, trie, ScheduledSnapshotOptions{
					Interval:      time.Hour,
					SnapshotPath:  snapshotPath,
					Format:        SnapshotFormatBinary,
					RunImmediately: false,
				})
				if err != nil {
					b.Fatalf("StartScheduledSnapshots() error = %v", err)
				}
				defer scheduler.Close()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if scheduler != nil {
					if _, err := scheduler.RunOnce(context.Background()); err != nil {
						b.Fatalf("RunOnce() error = %v", err)
					}
				} else if err := journal.SaveSnapshotWithFormat(trie, snapshotPath, SnapshotFormatBinary); err != nil {
					b.Fatalf("SaveSnapshotWithFormat() error = %v", err)
				}
			}
		})
	}
}
