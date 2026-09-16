package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkTT011RestorePointInTime(b *testing.B) {
	bundlePath, targetSequence := benchmarkTT011Bundle(b)
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := RestoreBackupBundle(bundlePath, filepath.Join(root, strconv.Itoa(index)), BackupBundleRestoreOptions{
			MaxJournalSequence: targetSequence,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
