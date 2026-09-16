package hatCache

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreBackupBundlePointInTimeByJournalSequence(t *testing.T) {
	for _, journalFormat := range []CommandJournalFormat{CommandJournalFormatJSON, CommandJournalFormatBinary} {
		t.Run(string(journalFormat), func(t *testing.T) {
			source := newTestTrie(t)
			defer source.Destroy()
			journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), journalFormat)
			if err != nil {
				t.Fatal(err)
			}
			if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "state", Value: "snapshot"}); !response.OK {
				t.Fatalf("snapshot SETSTR response = %#v", response)
			}
			baseBundlePath := filepath.Join(t.TempDir(), "base.tar.gz")
			if _, err := CreateBackupBundle(baseBundlePath, source, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
				t.Fatalf("CreateBackupBundle() error = %v", err)
			}
			if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "state", Value: "point-in-time"}); !response.OK {
				t.Fatalf("point-in-time SETSTR response = %#v", response)
			}
			if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "future", Value: "must-not-restore"}); !response.OK {
				t.Fatalf("future SETSTR response = %#v", response)
			}
			journalPath := journal.path
			if err := journal.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			bundlePath := rebuildBackupBundleWithJournal(t, baseBundlePath, journalPath)

			report, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{
				MaxJournalSequence: 2,
			})
			if err != nil {
				t.Fatalf("RestoreBackupBundle(point-in-time) error = %v", err)
			}
			if report.JournalSequence != 2 || report.RecoveredKeys != 1 {
				t.Fatalf("restore report sequence/keys = %d/%d, want 2/1", report.JournalSequence, report.RecoveredKeys)
			}
			restored := newTestTrie(t)
			defer restored.Destroy()
			metadata, err := restored.LoadSnapshotWithMetadata(report.Snapshot)
			if err != nil {
				t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
			}
			restoredJournal, err := OpenCommandJournalWithFormat(report.Journal, journalFormat)
			if err != nil {
				t.Fatalf("OpenCommandJournal() error = %v", err)
			}
			if sequence, err := restoredJournal.Replay(restored, metadata.JournalSequence); err != nil || sequence != 2 {
				_ = restoredJournal.Close()
				t.Fatalf("Replay() = %d/%v, want 2/nil", sequence, err)
			}
			if err := restoredJournal.Close(); err != nil {
				t.Fatalf("Close(restored journal) error = %v", err)
			}
			if got := restored.GetString("state"); got != "point-in-time" {
				t.Fatalf("restored state = %q, want point-in-time", got)
			}
			if restored.Exists("future") {
				t.Fatal("future journal mutation was restored")
			}
		})
	}
}

func TestRestoreBackupBundlePointInTimeAtSnapshotCheckpoint(t *testing.T) {
	source := newTestTrie(t)
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "state", Value: "snapshot"}); !response.OK {
		t.Fatalf("snapshot SETSTR response = %#v", response)
	}
	baseBundlePath := filepath.Join(t.TempDir(), "base.tar.gz")
	if _, err := CreateBackupBundle(baseBundlePath, source, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "future", Value: "must-not-restore"}); !response.OK {
		t.Fatalf("future SETSTR response = %#v", response)
	}
	journalPath := journal.path
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	bundlePath := rebuildBackupBundleWithJournal(t, baseBundlePath, journalPath)

	report, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{
		MaxJournalSequence: 1,
	})
	if err != nil {
		t.Fatalf("RestoreBackupBundle(snapshot checkpoint) error = %v", err)
	}
	if report.JournalSequence != 1 || report.RecoveredKeys != 1 {
		t.Fatalf("restore report sequence/keys = %d/%d, want 1/1", report.JournalSequence, report.RecoveredKeys)
	}
	restored := newTestTrie(t)
	defer restored.Destroy()
	metadata, err := restored.LoadSnapshotWithMetadata(report.Snapshot)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	restoredJournal, err := OpenCommandJournal(report.Journal)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	if sequence, err := restoredJournal.Replay(restored, metadata.JournalSequence); err != nil || sequence != 1 {
		_ = restoredJournal.Close()
		t.Fatalf("Replay() = %d/%v, want 1/nil", sequence, err)
	}
	if err := restoredJournal.Close(); err != nil {
		t.Fatalf("Close(restored journal) error = %v", err)
	}
	if got := restored.GetString("state"); got != "snapshot" {
		t.Fatalf("restored state = %q, want snapshot", got)
	}
	if restored.Exists("future") {
		t.Fatal("future journal mutation was restored")
	}
}

func TestRestoreBackupBundlePointInTimeRejectsSequenceBeforeSnapshot(t *testing.T) {
	source := newTestTrie(t)
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"}); !response.OK {
		t.Fatalf("first SETSTR response = %#v", response)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"}); !response.OK {
		t.Fatalf("second SETSTR response = %#v", response)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, err = RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{MaxJournalSequence: 1})
	if err == nil || !containsRestoreError(err, "before snapshot checkpoint") {
		t.Fatalf("RestoreBackupBundle(before snapshot) error = %v", err)
	}
}

func TestRestoreBackupBundlePointInTimeRejectsSequenceAfterBackup(t *testing.T) {
	source := newTestTrie(t)
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"}); !response.OK {
		t.Fatalf("SETSTR response = %#v", response)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, err = RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{MaxJournalSequence: 2})
	if err == nil || !containsRestoreError(err, "after backup sequence") {
		t.Fatalf("RestoreBackupBundle(after backup) error = %v", err)
	}
}

func containsRestoreError(err error, text string) bool {
	return err != nil && len(text) > 0 && strings.Contains(err.Error(), text)
}
