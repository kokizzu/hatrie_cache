package hatriecache

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPullCommandJournalRecoveryReusesObjectsAndRestoresLatest(t *testing.T) {
	source := newTestTrie(t)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "source.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	const keys = 512
	for index := 0; index < keys; index++ {
		key := fmt.Sprintf("recovery:%04d", index)
		source.UpsertString(key, strings.Repeat("x", 256))
		dirty.Mark(key)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "journal-sequence", Value: "base"}); !response.OK {
		t.Fatalf("base journal response = %#v", response)
	}
	dirty.Mark("journal-sequence")

	sourceRepository := filepath.Join(t.TempDir(), "source-repository")
	handler := NewMonitoringHandler(source, MonitoringOptions{
		ReplicationAuthToken:          "recovery-secret",
		Journal:                       journal,
		LevelDBStore:                  store,
		LevelDBDirtyTracker:           dirty,
		JournalRecoveryRepositoryPath: sourceRepository,
	}).Handler()
	server := httptest.NewServer(handler)
	defer server.Close()

	localRepository := filepath.Join(t.TempDir(), "local-repository")
	first, err := PullCommandJournalRecovery(context.Background(), server.URL, "recovery-secret", server.Client(), localRepository, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.DownloadedObjects == 0 || first.DownloadedBytes == 0 || first.ReusedObjects != 0 {
		t.Fatalf("first recovery transfer = %#v, want full object download", first)
	}
	for _, file := range first.Manifest.Files {
		if file.Size == 0 {
			continue
		}
		objectPath, err := backupRepositoryObjectPath(localRepository, file.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(objectPath, make([]byte, file.Size), 0o600); err != nil {
			t.Fatal(err)
		}
		break
	}

	for index := 0; index < 5; index++ {
		key := fmt.Sprintf("recovery:%04d", index)
		source.UpsertString(key, strings.Repeat("y", 256))
		dirty.Mark(key)
	}
	source.Delete("recovery:0511")
	dirty.Mark("recovery:0511")
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "journal-sequence", Value: "changed"}); !response.OK {
		t.Fatalf("changed journal response = %#v", response)
	}
	dirty.Mark("journal-sequence")

	second, err := PullCommandJournalRecovery(context.Background(), server.URL, "recovery-secret", server.Client(), localRepository, first.Manifest.JournalSequence)
	if err != nil {
		t.Fatal(err)
	}
	if second.Manifest.JournalSequence != 2 || !second.Manifest.Incremental || second.ReusedObjects == 0 || second.ReusedBytes <= second.DownloadedBytes {
		t.Fatalf("second recovery transfer = %#v, want mostly reused incremental objects", second)
	}

	restoreDir := filepath.Join(t.TempDir(), "restore")
	report, err := RestoreBackupRepository(localRepository, second.Manifest.BackupID, restoreDir, BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatal(err)
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
	if got := restored.GetString("recovery:0000"); got != strings.Repeat("y", 256) {
		t.Fatalf("changed recovery value = %q", got)
	}
	if restored.Exists("recovery:0511") {
		t.Fatal("deleted recovery key remained in restored repository")
	}
	if got := restored.GetString("journal-sequence"); got != "changed" {
		t.Fatalf("journal-sequence value = %q, want changed", got)
	}
}

func TestCommandJournalReplaceWithPersistentStoreIsExact(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("current", "value")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "recovery.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(source); err != nil {
		t.Fatal(err)
	}

	target := newTestTrie(t)
	target.UpsertString("stale", "remove")
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	loaded, err := journal.ReplaceWithPersistentStore(target, store, 42)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 1 || target.GetString("current") != "value" || target.Exists("stale") {
		t.Fatalf("persistent replacement = loaded:%d entries:%#v", loaded, target.Entries(true))
	}
	if journal.Sequence() != 42 {
		t.Fatalf("replacement journal sequence = %d, want 42", journal.Sequence())
	}
}

func TestValidateCommandJournalRecoveryManifestRejectsUnexpectedPaths(t *testing.T) {
	manifest := BackupBundleManifest{
		Version:        BackupBundleVersion,
		Mode:           BackupModePebbleIncremental,
		StorageBackend: string(StorageBackendPebble),
		StorageFormat:  string(DefaultStorageFormat),
		Store:          "../outside",
		BackupID:       strings.Repeat("0", 64),
	}
	if err := validateCommandJournalRecoveryManifest(manifest); err == nil {
		t.Fatal("unexpected recovery store path was accepted")
	}
	manifest.Store = backupBundleStorePath
	manifest.Journal = "../outside.journal"
	if err := validateCommandJournalRecoveryManifest(manifest); err == nil {
		t.Fatal("unexpected recovery journal path was accepted")
	}
}
