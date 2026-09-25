//go:build mz003baseline

package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkMZ003SnapshotRestoreExisting(b *testing.B) {
	source := CreateHatTrie()
	b.Cleanup(source.Destroy)
	for index := 0; index < 256; index++ {
		source.UpsertString(fmt.Sprintf("mz003:%04d", index), fmt.Sprintf("value-%04d", index))
	}
	path := filepath.Join(b.TempDir(), "snapshot.hc")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
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
