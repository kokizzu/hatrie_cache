package hatBackup

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCH022ObjectStoreBackupAppendsConfiguredManifestCatalog(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "part", "rows.bin"), []byte("payload"))
	store := newCH022ObjectStore()
	catalogPath := filepath.Join(t.TempDir(), "catalog", "manifests.log")
	catalog, err := NewBackupManifestCatalog(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		Layout:          ObjectStoreLayoutContentAddressed,
		ManifestCatalog: catalog,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:              ModePebbleIncremental,
		BackupID:          "base",
		Store:             "pebble",
		StorageBackend:    "pebble",
		StorageFormat:     "v1",
		StorageIdentity:   "test-store",
		StorageGeneration: 1,
	}); err != nil {
		t.Fatalf("base Backup() error = %v", err)
	}
	if _, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:              ModePebbleIncremental,
		BackupID:          "next",
		Store:             "pebble",
		StorageBackend:    "pebble",
		StorageFormat:     "v1",
		ParentBackupID:    "base",
		Incremental:       true,
		StorageIdentity:   "test-store",
		StorageGeneration: 1,
	}); err != nil {
		t.Fatalf("incremental Backup() error = %v", err)
	}

	reopened, err := NewBackupManifestCatalog(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := reopened.Load()
	if err != nil {
		t.Fatalf("reopened catalog Load() error = %v", err)
	}
	if len(manifests) != 2 || manifests[0].BackupID != "base" || manifests[1].BackupID != "next" {
		t.Fatalf("reopened catalog manifests = %+v", manifests)
	}
	plan, err := reopened.Plan("next")
	if err != nil {
		t.Fatalf("reopened catalog Plan() error = %v", err)
	}
	if len(plan.Manifests) != 2 || plan.Manifests[0].BackupID != "base" || plan.Manifests[1].BackupID != "next" {
		t.Fatalf("reopened catalog plan = %+v", plan.Manifests)
	}
}

func TestCH022ObjectStoreBackupCatalogSkipsNonIncrementalManifest(t *testing.T) {
	root := t.TempDir()
	writeCH022File(t, filepath.Join(root, "snapshot", "rows.bin"), []byte("snapshot"))
	store := newCH022ObjectStore()
	catalog, err := NewBackupManifestCatalog(filepath.Join(t.TempDir(), "catalog", "manifests.log"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewObjectStoreTargetWithOptions(store, "backup", ObjectStoreTargetOptions{
		ManifestCatalog: catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Backup(context.Background(), root, BundleManifest{
		Mode:     ModeSnapshot,
		BackupID: "snapshot",
	}); err != nil {
		t.Fatalf("snapshot Backup() error = %v", err)
	}
	manifests, err := catalog.Load()
	if err != nil {
		t.Fatalf("catalog Load() error = %v", err)
	}
	if len(manifests) != 0 {
		t.Fatalf("catalog manifests = %+v, want empty for snapshot backup", manifests)
	}
}
