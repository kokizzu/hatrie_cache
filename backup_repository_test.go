package hatriecache

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/internal/jsonwire"
)

func TestIncrementalBackupRepositoryReusesObjectsAndRestoresLatest(t *testing.T) {
	trie := newTestTrie(t)
	for index := 0; index < 512; index++ {
		trie.UpsertString(backupRepositoryTestKey(index), strings.Repeat(string(rune('a'+index%26)), 256))
	}
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")

	base, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:             BackupModePebbleIncremental,
		PersistentStore:  store,
		DirtyTracker:     tracker,
		RepositoryRetain: 4,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle(base repository) error = %v", err)
	}
	if base.BackupID == "" || base.Incremental || base.ParentBackupID != "" || base.NewObjects == 0 || base.NewObjectBytes == 0 {
		t.Fatalf("base repository manifest = %#v", base)
	}

	trie.UpsertString("backup:key:7", "updated")
	tracker.Mark("backup:key:7")
	trie.Delete("backup:key:9")
	tracker.Mark("backup:key:9")
	second, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:             BackupModePebbleIncremental,
		PersistentStore:  store,
		DirtyTracker:     tracker,
		RepositoryRetain: 4,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle(incremental repository) error = %v", err)
	}
	if !second.Incremental || second.ParentBackupID != base.BackupID || second.StorageGeneration != base.StorageGeneration {
		t.Fatalf("incremental repository manifest = %#v, base = %#v", second, base)
	}
	if second.ReusedObjects == 0 || second.ReusedObjectBytes == 0 || second.NewObjectBytes >= backupRepositoryLogicalBytes(second.Files) {
		t.Fatalf("incremental object reuse = %#v", second)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("dirty tracker pending = %d, want 0 after committed backup", tracker.Pending())
	}

	doctor, err := VerifyBackupPath(repository)
	if err != nil {
		t.Fatalf("VerifyBackupPath(repository) error = %v", err)
	}
	if !doctor.OK || doctor.Kind != "repository" || doctor.BackupID != second.BackupID || doctor.RecoveredKeys != 511 {
		t.Fatalf("repository doctor = %#v", doctor)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	report, err := RestoreBackupBundle(repository, dataDir, BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreBackupBundle(repository) error = %v", err)
	}
	if report.BackupID != second.BackupID || report.Store == "" || report.RecoveredKeys != 511 {
		t.Fatalf("repository restore report = %#v", report)
	}
	restoredStore, err := OpenPersistentStore(report.Store)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredStore.Close()
	restored := newTestTrie(t)
	if _, err := restoredStore.Load(restored); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("backup:key:7"); got != "updated" {
		t.Fatalf("restored updated value = %q", got)
	}
	if restored.Exists("backup:key:9") {
		t.Fatal("restored deleted key")
	}
}

func TestIncrementalBackupRepositoryStartsNewBaseAfterGenerationChange(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "first")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")
	base, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{Mode: BackupModePebbleIncremental, PersistentStore: store, DirtyTracker: tracker})
	if err != nil {
		t.Fatal(err)
	}

	trie.UpsertString("name", "second")
	tracker.Mark("name")
	if err := store.Save(trie); err != nil {
		t.Fatal(err)
	}
	next, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{Mode: BackupModePebbleIncremental, PersistentStore: store, DirtyTracker: tracker})
	if err != nil {
		t.Fatal(err)
	}
	if next.Incremental || next.StorageGeneration == base.StorageGeneration || next.ParentBackupID != base.BackupID {
		t.Fatalf("new base manifest = %#v, previous = %#v", next, base)
	}
}

func TestIncrementalBackupRepositoryPrunesManifestsAndObjects(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "value-0")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")
	for index := 0; index < 4; index++ {
		trie.UpsertString("name", "value-"+string(rune('0'+index)))
		tracker.Mark("name")
		if _, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
			Mode:             BackupModePebbleIncremental,
			PersistentStore:  store,
			DirtyTracker:     tracker,
			RepositoryRetain: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(repository, backupRepositoryManifestsPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("retained manifests = %d, want 2", len(entries))
	}
	if _, err := VerifyBackupPath(repository); err != nil {
		t.Fatalf("VerifyBackupPath(pruned repository) error = %v", err)
	}
}

func TestIncrementalBackupRepositoryDetectsCorruptObject(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "ivi")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	manifest, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	})
	if err != nil {
		t.Fatal(err)
	}
	objectPath, err := backupRepositoryObjectPath(repository, manifest.Files[0].SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackupPath(repository); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("VerifyBackupPath(corrupt repository) error = %v", err)
	}
}

func TestIncrementalBackupRepositoryDetectsModifiedManifest(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "ivi")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	manifest, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest.StorageIdentity = "modified"
	data, err := jsonwire.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(repository, backupRepositoryManifestsPath, manifest.BackupID+".json")
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackupPath(repository); err == nil || !strings.Contains(err.Error(), "manifest checksum mismatch") {
		t.Fatalf("VerifyBackupPath(modified manifest) error = %v", err)
	}
}

func TestIncrementalBackupRepositoryRejectsModifiedDescriptor(t *testing.T) {
	trie := newTestTrie(t)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	if _, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, backupRepositoryDescriptorPath), []byte(`{"version":1,"format":"modified"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackupPath(repository); err == nil || !strings.Contains(err.Error(), "unsupported backup repository format") {
		t.Fatalf("VerifyBackupPath(modified descriptor) error = %v", err)
	}
}

func TestIncrementalBackupRepositoryRequiresDirtyTracker(t *testing.T) {
	trie := newTestTrie(t)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = CreateBackupBundle(filepath.Join(t.TempDir(), "repository"), trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
	})
	if err == nil || !strings.Contains(err.Error(), "dirty tracker") {
		t.Fatalf("CreateBackupBundle(repository without dirty tracker) error = %v", err)
	}
}

func backupRepositoryTestKey(index int) string {
	return "backup:key:" + strconv.Itoa(index)
}

func backupRepositoryLogicalBytes(files []BackupBundleFile) int64 {
	var total int64
	for _, file := range files {
		total += file.Size
	}
	return total
}
