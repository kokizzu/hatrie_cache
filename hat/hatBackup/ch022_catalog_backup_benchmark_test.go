package hatBackup

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkCH022ObjectStoreBackupNoCatalog(b *testing.B) {
	root := b.TempDir()
	writeCH022CatalogBenchmarkFile(b, filepath.Join(root, "part", "rows.bin"), "catalog benchmark payload")
	store := newCH022ObjectStore()
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		Layout: ObjectStoreLayoutContentAddressed,
	})
	if err != nil {
		b.Fatal(err)
	}
	benchmarkCH022ObjectStoreBackups(b, target, root)
}

func BenchmarkCH022ObjectStoreBackupWithCatalog(b *testing.B) {
	root := b.TempDir()
	writeCH022CatalogBenchmarkFile(b, filepath.Join(root, "part", "rows.bin"), "catalog benchmark payload")
	store := newCH022ObjectStore()
	catalog, err := NewBackupManifestCatalog(filepath.Join(b.TempDir(), "catalog", "manifests.log"))
	if err != nil {
		b.Fatal(err)
	}
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		Layout:          ObjectStoreLayoutContentAddressed,
		ManifestCatalog: catalog,
	})
	if err != nil {
		b.Fatal(err)
	}
	benchmarkCH022ObjectStoreBackups(b, target, root)
}

func benchmarkCH022ObjectStoreBackups(b *testing.B, target *ObjectStoreTarget, root string) {
	b.Helper()
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

func writeCH022CatalogBenchmarkFile(b testing.TB, path, payload string) {
	b.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		b.Fatal(err)
	}
}
