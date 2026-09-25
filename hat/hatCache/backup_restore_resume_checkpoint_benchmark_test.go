package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkRestoreBackupBundleResumeCheckpoint(b *testing.B) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("resume:benchmark", "value")
	bundlePath := filepath.Join(b.TempDir(), "benchmark.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
	}); err != nil {
		b.Fatal(err)
	}
	for _, variant := range []struct {
		name   string
		resume bool
	}{
		{name: "fresh", resume: false},
		{name: "resume-checkpoint", resume: true},
	} {
		b.Run(variant.name, func(b *testing.B) {
			root := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				dataDir := filepath.Join(root, fmt.Sprintf("restore-%d", index))
				if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{Resume: variant.resume}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
