package hatCache

import (
	"path/filepath"
	"testing"
)

func TestTT023SelectivePebbleCheckpointRestoreFiltersPartition(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("region:sg/user:1", "Singapore")
	source.UpsertCounter("region:sg/visits", 42)
	source.UpsertBytes("region:sg/blob", []byte("payload"))
	source.UpsertString("region:us/user:1", "United States")
	source.UpsertString("global/config", "excluded")

	root := t.TempDir()
	store, err := OpenPebbleStoreWithFormat(filepath.Join(root, "source.leveldb"), StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPebbleStoreWithFormat() error = %v", err)
	}
	if err := store.Save(source); err != nil {
		store.Close()
		t.Fatalf("Save() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	bundlePath := filepath.Join(root, "checkpoint.tar.gz")
	store, err = OpenPebbleStoreWithFormat(filepath.Join(root, "source.leveldb"), StorageFormatBinary)
	if err != nil {
		t.Fatalf("reopen store error = %v", err)
	}
	defer store.Close()
	_, err = CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
		Partition: BackupPartitionMetadata{
			Mode:        "partitioned",
			Local:       true,
			Partitions:  []string{"sg", "us"},
			KeyPrefixes: []string{"region:sg/", "region:us/"},
		},
		PartitionLocal: true,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	selector := BackupPartitionMetadata{
		Mode:        "partitioned",
		Local:       true,
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:sg/"},
	}
	restoredDir := filepath.Join(root, "restored")
	report, err := RestoreBackupBundle(bundlePath, restoredDir, BackupBundleRestoreOptions{Partition: &selector})
	if err != nil {
		t.Fatalf("RestoreBackupBundle() error = %v", err)
	}
	if report.RecoveredKeys != 3 {
		t.Fatalf("RecoveredKeys = %d, want 3", report.RecoveredKeys)
	}

	restoredStore, err := OpenPebbleStoreWithFormat(report.Store, StorageFormatBinary)
	if err != nil {
		t.Fatalf("open restored store error = %v", err)
	}
	defer restoredStore.Close()
	restored := CreateHatTrie()
	defer restored.Destroy()
	if _, err := restoredStore.Load(restored); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !restored.Exists("region:sg/user:1") || !restored.Exists("region:sg/visits") || !restored.Exists("region:sg/blob") {
		t.Fatal("selected Singapore values were not restored")
	}
	if restored.Exists("region:us/user:1") || restored.Exists("global/config") {
		t.Fatal("unselected values were restored")
	}
}

func BenchmarkTT023SelectivePebbleCheckpointRestore(b *testing.B) {
	source := CreateHatTrie()
	b.Cleanup(source.Destroy)
	source.UpsertString("region:sg/user:1", "Singapore")
	source.UpsertCounter("region:sg/visits", 42)
	source.UpsertBytes("region:sg/blob", []byte("payload"))
	source.UpsertString("region:us/user:1", "United States")
	source.UpsertString("global/config", "excluded")

	store, err := OpenPebbleStore(b.TempDir() + "/source.leveldb")
	if err != nil {
		b.Fatal(err)
	}
	bundlePath := b.TempDir() + "/selected.tar.gz"
	_, err = CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
		Partition: BackupPartitionMetadata{
			Mode:        "partitioned",
			Local:       true,
			Partitions:  []string{"sg", "us"},
			KeyPrefixes: []string{"region:sg/", "region:us/"},
		},
		PartitionLocal: true,
	})
	if closeErr := store.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		b.Fatal(err)
	}
	selector := BackupPartitionMetadata{
		Mode:        "partitioned",
		Local:       true,
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:sg/"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := RestoreBackupBundle(bundlePath, b.TempDir(), BackupBundleRestoreOptions{Partition: &selector}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTT023FullPebbleCheckpointRestore(b *testing.B) {
	source := CreateHatTrie()
	b.Cleanup(source.Destroy)
	source.UpsertString("region:sg/user:1", "Singapore")
	source.UpsertCounter("region:sg/visits", 42)
	source.UpsertBytes("region:sg/blob", []byte("payload"))
	source.UpsertString("region:us/user:1", "United States")
	source.UpsertString("global/config", "included")

	store, err := OpenPebbleStore(b.TempDir() + "/source.leveldb")
	if err != nil {
		b.Fatal(err)
	}
	bundlePath := b.TempDir() + "/full.tar.gz"
	_, err = CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
	})
	if closeErr := store.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := RestoreBackupBundle(bundlePath, b.TempDir(), BackupBundleRestoreOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
