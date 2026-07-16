package hatriecache

import (
	"path/filepath"
	"testing"
)

func TestVerifyBackupPathChecksAtomicBundle(t *testing.T) {
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

	report, err := VerifyBackupPath(bundlePath)
	if err != nil {
		t.Fatalf("VerifyBackupPath(bundle) error = %v", err)
	}
	if !report.OK || report.Kind != "bundle" || report.Snapshot == nil || !report.Snapshot.OK || report.Journal == nil || !report.Journal.OK {
		t.Fatalf("bundle doctor report = %#v, want ok snapshot and journal", report)
	}
	if report.RecoveredKeys != 1 || report.JournalSequence != 1 {
		t.Fatalf("bundle recovered keys/sequence = %d/%d, want 1/1", report.RecoveredKeys, report.JournalSequence)
	}
}

func TestVerifyBackupPathChecksDirectorySnapshotJournal(t *testing.T) {
	dir := t.TempDir()
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(dir, "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "before", Value: "snapshot"}); !got.OK {
		t.Fatalf("journaled before SETSTR response = %#v, want ok", got)
	}
	if err := journal.SaveSnapshotWithFormat(ht, filepath.Join(dir, "snapshot.hc"), SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "journal"}); !got.OK {
		t.Fatalf("journaled after SETSTR response = %#v, want ok", got)
	}

	report, err := VerifyBackupPath(dir)
	if err != nil {
		t.Fatalf("VerifyBackupPath(directory) error = %v", err)
	}
	if !report.OK || report.Kind != "directory" || report.Snapshot == nil || report.Journal == nil {
		t.Fatalf("directory doctor report = %#v, want ok snapshot and journal", report)
	}
	if report.RecoveredKeys != 2 || report.JournalSequence != 2 {
		t.Fatalf("directory recovered keys/sequence = %d/%d, want 2/2", report.RecoveredKeys, report.JournalSequence)
	}
}

func TestVerifyBackupPathChecksDirectoryJournalOnly(t *testing.T) {
	dir := t.TempDir()
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(dir, "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "only", Value: "journal"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}

	report, err := VerifyBackupPath(dir)
	if err != nil {
		t.Fatalf("VerifyBackupPath(journal only) error = %v", err)
	}
	if !report.OK || report.Journal == nil || report.RecoveredKeys != 1 || report.JournalSequence != 1 {
		t.Fatalf("journal-only doctor report = %#v, want one recovered key at sequence 1", report)
	}
}

func TestVerifyBackupPathRejectsEmptyDirectory(t *testing.T) {
	_, err := VerifyBackupPath(t.TempDir())
	if err == nil {
		t.Fatal("VerifyBackupPath(empty directory) error = nil, want rejection")
	}
}
