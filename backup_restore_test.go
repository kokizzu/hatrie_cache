package hatriecache

import (
	"path/filepath"
	"testing"
)

func TestRestoreBackupBundleVerifiesAndWritesSnapshotJournal(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
	report, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreBackupBundle() error = %v", err)
	}
	if !report.OK || report.RecoveredKeys != 1 || report.JournalSequence != 1 {
		t.Fatalf("restore report = %#v, want one recovered key at sequence 1", report)
	}
	restored := newTestTrie(t)
	if _, err := restored.LoadSnapshotWithMetadata(filepath.Join(dataDir, "snapshot.hc")); err != nil {
		t.Fatalf("LoadSnapshotWithMetadata(restored) error = %v", err)
	}
	if got := restored.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("restored GET response = %#v, want ivi", got)
	}
	if _, err := OpenCommandJournal(filepath.Join(dataDir, "commands.journal")); err != nil {
		t.Fatalf("OpenCommandJournal(restored) error = %v", err)
	}
}

func TestRestoreBackupBundleRejectsNonEmptyDataDirByDefault(t *testing.T) {
	ht := newTestTrie(t)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	dataDir := t.TempDir()
	writeTestFile(t, filepath.Join(dataDir, "existing"), "keep")
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{}); err == nil {
		t.Fatal("RestoreBackupBundle(non-empty) error = nil, want rejection")
	}
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("RestoreBackupBundle(overwrite) error = %v", err)
	}
}
