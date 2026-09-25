//go:build !mz003baseline

package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkMZ003SnapshotRestoreExisting(b *testing.B) {
	source, path, snapshotBytes := prepareMZ003SnapshotRestoreBenchmark(b)
	b.Cleanup(source.Destroy)
	b.SetBytes(snapshotBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		target := CreateHatTrie()
		if _, err := target.LoadSnapshotWithMetadata(path); err != nil {
			target.Destroy()
			b.Fatal(err)
		}
		target.Destroy()
	}
}

func BenchmarkMZ003SnapshotRestoreWithProgress(b *testing.B) {
	source, path, snapshotBytes := prepareMZ003SnapshotRestoreBenchmark(b)
	b.Cleanup(source.Destroy)
	progress := func(SnapshotRestoreProgress) error { return nil }
	b.SetBytes(snapshotBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		target := CreateHatTrie()
		if _, err := target.LoadSnapshotWithProgress(path, progress); err != nil {
			target.Destroy()
			b.Fatal(err)
		}
		target.Destroy()
	}
}

func prepareMZ003SnapshotRestoreBenchmark(b *testing.B) (*HatTrie, string, int64) {
	b.Helper()
	source := CreateHatTrie()
	for index := 0; index < 256; index++ {
		source.UpsertString(fmt.Sprintf("mz003:%04d", index), fmt.Sprintf("value-%04d", index))
	}
	path := filepath.Join(b.TempDir(), "snapshot.hc")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		source.Destroy()
		b.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		source.Destroy()
		b.Fatal(err)
	}
	return source, path, info.Size()
}
