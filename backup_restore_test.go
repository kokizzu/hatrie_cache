package hatriecache

import (
	"os"
	"path/filepath"
	"strings"
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

func TestRestoreBackupBundleAtomicallyReplacesDestination(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "new")
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(dataDir, "old-only"), "keep until commit")
	before, err := os.Stat(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Fatal("restore reused destination directory instead of publishing staged data")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "old-only")); !os.IsNotExist(err) {
		t.Fatalf("old-only file stat error = %v, want not exist", err)
	}
}

func TestRestoreBackupBundleSemanticFailurePreservesDestination(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("us:name", "wrong partition")
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
		Partition: BackupPartitionMetadata{
			Mode:        "partitioned",
			Partitions:  []string{"sg"},
			KeyPrefixes: []string{"sg:"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(dataDir, "old-only"), "preserve")
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{Overwrite: true}); err == nil || !strings.Contains(err.Error(), "partition metadata") {
		t.Fatalf("RestoreBackupBundle(invalid partition) error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dataDir, "old-only"))
	if err != nil || string(data) != "preserve" {
		t.Fatalf("preserved file = %q/%v", data, err)
	}
	assertNoRestoreStagingDirectories(t, parent)
}

func TestRestoreBackupBundleRejectsSymlinkDestination(t *testing.T) {
	ht := newTestTrie(t)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(target, "old-only"), "preserve")
	dataDir := filepath.Join(parent, "data")
	if err := os.Symlink(target, dataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{Overwrite: true}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("RestoreBackupBundle(symlink destination) error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "old-only"))
	if err != nil || string(data) != "preserve" {
		t.Fatalf("symlink target file = %q/%v", data, err)
	}
}

func TestRestoreBackupBundleRejectsSymlinkParent(t *testing.T) {
	ht := newTestTrie(t)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	targetParent := filepath.Join(parent, "target")
	if err := os.Mkdir(targetParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedParent := filepath.Join(parent, "linked")
	if err := os.Symlink(targetParent, linkedParent); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(linkedParent, "data")
	if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("RestoreBackupBundle(symlink parent) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(targetParent, "data")); !os.IsNotExist(err) {
		t.Fatalf("redirected restore target stat error = %v, want not exist", err)
	}
}

func assertNoRestoreStagingDirectories(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".restore-") {
			t.Fatalf("restore staging directory remains: %s", entry.Name())
		}
	}
}

func TestRehearseRestoreRestoresBundleInWorkDir(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	workDir := filepath.Join(t.TempDir(), "rehearsal")
	report, err := RehearseRestore(bundlePath, RestoreRehearsalOptions{WorkDir: workDir})
	if err != nil {
		t.Fatalf("RehearseRestore(bundle) error = %v", err)
	}
	if !report.OK || report.SourceKind != "bundle" || report.RecoveredKeys != 1 || !report.WorkDirKept {
		t.Fatalf("rehearsal report = %#v, want ok bundle with kept work dir", report)
	}
	if _, err := os.Stat(filepath.Join(workDir, "data", "snapshot.hc")); err != nil {
		t.Fatalf("rehearsal restored snapshot missing: %v", err)
	}
}

func TestRehearseRestoreCopiesDirectoryBackup(t *testing.T) {
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

	report, err := RehearseRestore(dir, RestoreRehearsalOptions{WorkDir: filepath.Join(t.TempDir(), "rehearsal")})
	if err != nil {
		t.Fatalf("RehearseRestore(directory) error = %v", err)
	}
	if !report.OK || report.SourceKind != "directory" || report.RecoveredKeys != 2 || report.JournalSequence != 2 {
		t.Fatalf("rehearsal report = %#v, want directory with two recovered keys", report)
	}
}
