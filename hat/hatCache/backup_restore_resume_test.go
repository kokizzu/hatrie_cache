package hatCache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreDestinationResumeReusesStaging(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "bundle.tar.gz")
	target := filepath.Join(root, "restored")

	first, err := prepareRestoreDestinationWithResume(source, target, false, true)
	if err != nil {
		t.Fatalf("prepareRestoreDestinationWithResume() error = %v", err)
	}
	marker := filepath.Join(first.StagingPath(), "partial.marker")
	if err := os.WriteFile(marker, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	second, err := prepareRestoreDestinationWithResume(source, target, false, true)
	if err != nil {
		t.Fatalf("prepareRestoreDestinationWithResume(retry) error = %v", err)
	}
	defer second.Cleanup()
	if second.StagingPath() != first.StagingPath() {
		t.Fatalf("retry staging path = %q, want %q", second.StagingPath(), first.StagingPath())
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("resume marker was not retained: %v", err)
	}
}

func TestRestoreBackupBundleResumePublishesSnapshot(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("resume:key", "value")
	bundlePath := filepath.Join(t.TempDir(), "resume.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
	}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	report, err := RestoreBackupBundle(bundlePath, restoreDir, BackupBundleRestoreOptions{Resume: true})
	if err != nil {
		t.Fatalf("RestoreBackupBundle(Resume) error = %v", err)
	}
	if !report.OK || report.Snapshot == "" {
		t.Fatalf("restore report = %#v, want published snapshot", report)
	}
	checkpoint := filepath.Join(filepath.Dir(restoreDir), "."+filepath.Base(restoreDir)+".restore-resume")
	if _, err := os.Stat(checkpoint); !os.IsNotExist(err) {
		t.Fatalf("resume checkpoint stat error = %v, want absent after publish", err)
	}

	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(report.Snapshot); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := restored.GetString("resume:key"); got != "value" {
		t.Fatalf("restored resume:key = %q, want value", got)
	}
}

func TestRestoreBackupBundleResumeRepairsStaleFiles(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("resume:repair", "current")
	bundlePath := filepath.Join(t.TempDir(), "repair.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
	}); err != nil {
		t.Fatal(err)
	}
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	destination, err := prepareRestoreDestinationWithResume(bundlePath, restoreDir, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination.StagingPath(), "stale.tmp"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	staleSnapshot := filepath.Join(destination.StagingPath(), filepath.FromSlash(manifest.Snapshot))
	if err := os.MkdirAll(filepath.Dir(staleSnapshot), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staleSnapshot, []byte("incorrect"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := RestoreBackupBundle(bundlePath, restoreDir, BackupBundleRestoreOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "stale.tmp")); !os.IsNotExist(err) {
		t.Fatalf("stale file after restore error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(filepath.Join(restoreDir, filepath.FromSlash(manifest.Snapshot))); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("resume:repair"); got != "current" {
		t.Fatalf("repaired snapshot value = %q, want current", got)
	}
}

func TestRestoreBackupBundleResumeRetainsUnsafeCheckpoint(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("resume:unsafe", "value")
	bundlePath := filepath.Join(t.TempDir(), "unsafe.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
	}); err != nil {
		t.Fatal(err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	destination, err := prepareRestoreDestinationWithResume(bundlePath, restoreDir, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(destination.StagingPath(), "unsafe-link")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := RestoreBackupBundle(bundlePath, restoreDir, BackupBundleRestoreOptions{Resume: true}); err == nil {
		t.Fatal("RestoreBackupBundle accepted a symlinked checkpoint entry")
	}
	if _, err := os.Stat(destination.StagingPath()); err != nil {
		t.Fatalf("resume checkpoint was not retained: %v", err)
	}
}

func TestRestoreBackupRepositoryResumeRepairsStaleFiles(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("resume:repository", "value")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	if _, err := CreateBackupBundle(repository, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	}); err != nil {
		t.Fatal(err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	destination, err := prepareRestoreDestinationWithResume(repository, restoreDir, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination.StagingPath(), "stale.tmp"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := RestoreBackupBundle(repository, restoreDir, BackupBundleRestoreOptions{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "stale.tmp")); !os.IsNotExist(err) {
		t.Fatalf("stale repository file after restore error = %v", err)
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
	if got := restored.GetString("resume:repository"); got != "value" {
		t.Fatalf("restored repository value = %q, want value", got)
	}
}
