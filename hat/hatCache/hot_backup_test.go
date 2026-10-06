package hatCache

import (
	"path/filepath"
	"testing"
	"time"
)

func TestCreateHotBackupBundleCapturesOnlineSnapshotManifest(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("SETSTR response = %#v, want ok", response)
	}

	bundlePath := filepath.Join(t.TempDir(), "hot-backup.tar.gz")
	createdAt := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	result, err := CreateHotBackupBundle(bundlePath, trie, journal, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
		CreatedAt:      createdAt,
	})
	if err != nil {
		t.Fatalf("CreateHotBackupBundle() error = %v", err)
	}
	if result.Bundle.JournalSequence != 1 || result.Snapshot.JournalSequence != 1 {
		t.Fatalf("journal coordinates = bundle %d snapshot %d, want 1", result.Bundle.JournalSequence, result.Snapshot.JournalSequence)
	}
	if result.Bundle.SnapshotFormat != string(SnapshotFormatJSON) || result.Bundle.JournalFormat != string(CommandJournalFormatJSON) {
		t.Fatalf("formats = bundle snapshot %q journal %q", result.Bundle.SnapshotFormat, result.Bundle.JournalFormat)
	}
	if result.Bundle.CreatedAt != createdAt {
		t.Fatalf("created time = %v, want %v", result.Bundle.CreatedAt, createdAt)
	}
	snapshotFile := hotBackupBundleFile(t, result.Bundle, backupBundleSnapshotPath)
	if snapshotFile.Size != result.Snapshot.SizeBytes || snapshotFile.SHA256 != result.Snapshot.SHA256 {
		t.Fatalf("snapshot identity = %#v, want size=%d sha=%s", snapshotFile, result.Snapshot.SizeBytes, result.Snapshot.SHA256)
	}

	if _, err := VerifyBackupBundle(bundlePath); err != nil {
		t.Fatalf("VerifyBackupBundle() error = %v", err)
	}
	restoredDir := filepath.Join(t.TempDir(), "restored")
	if _, err := RestoreBackupBundle(bundlePath, restoredDir, BackupBundleRestoreOptions{}); err != nil {
		t.Fatalf("RestoreBackupBundle() error = %v", err)
	}
	restored := newTestTrie(t)
	if _, err := restored.LoadSnapshotWithMetadata(filepath.Join(restoredDir, backupBundleSnapshotPath)); err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if got := restored.GetString("name"); got != "ivi" {
		t.Fatalf("restored name = %q, want ivi", got)
	}
}

func hotBackupBundleFile(t *testing.T, manifest BackupBundleManifest, path string) BackupBundleFile {
	t.Helper()
	for _, file := range manifest.Files {
		if file.Path == path {
			return file
		}
	}
	t.Fatalf("manifest is missing %s", path)
	return BackupBundleFile{}
}
