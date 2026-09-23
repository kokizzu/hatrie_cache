package hatCache

import (
	"path/filepath"
	"testing"
)

func BenchmarkT214SnapshotTransferBaseline(b *testing.B) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("benchmark", "snapshot-transfer")
	target := CreateHatTrie()
	defer target.Destroy()
	path := filepath.Join(b.TempDir(), "snapshot.hc")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
			b.Fatalf("SaveSnapshotWithFormat() error = %v", err)
		}
		if err := target.LoadSnapshot(path); err != nil {
			b.Fatalf("LoadSnapshot() error = %v", err)
		}
	}
}
