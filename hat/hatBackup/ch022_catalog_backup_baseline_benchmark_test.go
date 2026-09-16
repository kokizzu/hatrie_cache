package hatBackup

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkCH022ObjectStoreBackupNoCatalogBaseline(b *testing.B) {
	root := b.TempDir()
	writeCH022CatalogBaselineBenchmarkFile(b, filepath.Join(root, "part", "rows.bin"), "catalog benchmark payload")
	store := newCH022ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := target.Backup(context.Background(), root, BundleManifest{
			Mode:              ModePebbleIncremental,
			BackupID:          "backup-" + strconv.Itoa(index),
			Store:             "pebble",
			StorageBackend:    "pebble",
			StorageFormat:     "v1",
			StorageIdentity:   "benchmark",
			StorageGeneration: 1,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func writeCH022CatalogBaselineBenchmarkFile(b testing.TB, path, payload string) {
	b.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		b.Fatal(err)
	}
}
