package hatCache

import (
	"encoding/json"
	"path/filepath"
	"testing"

	hatBackup "hatrie_cache/hat/hatBackup"
)

func TestCHU50BackupConsistencyBindsSnapshotAndJournal(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v, want ok", response)
	}

	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	consistency := manifest.Consistency
	if consistency == nil {
		t.Fatal("backup manifest consistency = nil, want part/WAL binding")
	}
	if consistency.Version != hatBackup.BundleConsistencyVersion {
		t.Fatalf("consistency version = %d, want %d", consistency.Version, hatBackup.BundleConsistencyVersion)
	}
	if consistency.PartSequence != 1 || consistency.JournalSequence != manifest.JournalSequence || consistency.JournalSequence != 1 {
		t.Fatalf("consistency part/journal sequence = %d/%d, manifest sequence = %d, want 1/1", consistency.PartSequence, consistency.JournalSequence, manifest.JournalSequence)
	}
	if len(consistency.Parts) != 1 {
		t.Fatalf("consistency parts = %#v, want one snapshot part", consistency.Parts)
	}
	part := consistency.Parts[0]
	if part.Kind != hatBackup.BundlePartKindSnapshot || part.Path != backupBundleSnapshotPath {
		t.Fatalf("snapshot part = %#v, want snapshot.hc snapshot part", part)
	}
	if consistency.Journal == nil || consistency.Journal.Path != backupBundleJournalPath || consistency.Journal.Format != string(CommandJournalFormatJSON) || consistency.Journal.Sequence != 1 {
		t.Fatalf("journal boundary = %#v, want JSON checkpoint at sequence 1", consistency.Journal)
	}

	files := readBackupBundleFiles(t, bundlePath)
	assertBundleFileChecksum(t, manifest, backupBundleSnapshotPath, files[backupBundleSnapshotPath])
	assertBundleFileChecksum(t, manifest, backupBundleJournalPath, files[backupBundleJournalPath])
	snapshotFile := bundleFileByPath(t, manifest, backupBundleSnapshotPath)
	journalFile := bundleFileByPath(t, manifest, backupBundleJournalPath)
	if part.Size != snapshotFile.Size || part.SHA256 != snapshotFile.SHA256 {
		t.Fatalf("snapshot part = %#v, file = %#v, want matching size/checksum", part, snapshotFile)
	}
	if consistency.Journal.Size != journalFile.Size || consistency.Journal.SHA256 != journalFile.SHA256 {
		t.Fatalf("journal boundary = %#v, file = %#v, want matching size/checksum", consistency.Journal, journalFile)
	}
	if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
		t.Fatalf("ValidateBundleConsistency() error = %v", err)
	}

	var bundled hatBackup.BundleManifest
	if err := json.Unmarshal(files[backupBundleManifestPath], &bundled); err != nil {
		t.Fatalf("Unmarshal(manifest.json) error = %v", err)
	}
	if err := hatBackup.ValidateBundleConsistency(bundled); err != nil {
		t.Fatalf("ValidateBundleConsistency(unmarshaled) error = %v", err)
	}
	if _, err := VerifyBackupBundle(bundlePath); err != nil {
		t.Fatalf("VerifyBackupBundle() error = %v", err)
	}
}

func TestCHU50BackupConsistencyRejectsManifestDrift(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v, want ok", response)
	}
	manifest, err := CreateBackupBundle(filepath.Join(t.TempDir(), "backup.tar.gz"), trie, journal, BackupBundleOptions{})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*hatBackup.BundleManifest)
	}{
		{
			name: "journal sequence",
			mutate: func(candidate *hatBackup.BundleManifest) {
				candidate.Consistency.JournalSequence++
			},
		},
		{
			name: "part checksum",
			mutate: func(candidate *hatBackup.BundleManifest) {
				candidate.Consistency.Parts[0].SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
			},
		},
		{
			name: "journal boundary",
			mutate: func(candidate *hatBackup.BundleManifest) {
				candidate.Consistency.Journal.Sequence++
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := manifest
			consistency := *manifest.Consistency
			consistency.Parts = append([]hatBackup.BundlePart(nil), manifest.Consistency.Parts...)
			if manifest.Consistency.Journal != nil {
				journalBoundary := *manifest.Consistency.Journal
				consistency.Journal = &journalBoundary
			}
			candidate.Consistency = &consistency
			test.mutate(&candidate)
			if err := hatBackup.ValidateBundleConsistency(candidate); err == nil {
				t.Fatal("ValidateBundleConsistency() error = nil, want manifest drift rejection")
			}
		})
	}
}

func TestCHU50BackupConsistencyBindsPebbleCheckpoint(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	for _, request := range []CacheCommandRequest{
		{Command: "SETSTR", Key: "name", Value: "ivi"},
		{Command: "SETINT", Key: "count", Value: "42"},
	} {
		if response := journal.ExecuteCommand(trie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%s) response = %#v, want ok", request.Command, response)
		}
	}
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()

	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if manifest.Consistency == nil || manifest.Consistency.JournalSequence != 2 || manifest.Consistency.Journal == nil {
		t.Fatalf("checkpoint consistency = %#v, want sequence 2 and journal boundary", manifest.Consistency)
	}
	if len(manifest.Consistency.Parts) < 2 {
		t.Fatalf("checkpoint consistency parts = %#v, want storage files and marker", manifest.Consistency.Parts)
	}
	storageParts, metadataParts := 0, 0
	for _, part := range manifest.Consistency.Parts {
		switch part.Kind {
		case hatBackup.BundlePartKindStorage:
			storageParts++
		case hatBackup.BundlePartKindMetadata:
			metadataParts++
		default:
			t.Fatalf("checkpoint part = %#v, want storage or metadata kind", part)
		}
	}
	if storageParts == 0 || metadataParts == 0 {
		t.Fatalf("checkpoint part kinds = storage %d, metadata %d, want both", storageParts, metadataParts)
	}
	if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
		t.Fatalf("ValidateBundleConsistency(checkpoint) error = %v", err)
	}
	if _, err := VerifyBackupBundle(bundlePath); err != nil {
		t.Fatalf("VerifyBackupBundle(checkpoint) error = %v", err)
	}
}

func TestCHU50BackupConsistencyRejectsSnapshotBoundaryDrift(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "ivi")
	root := t.TempDir()
	snapshotPath := filepath.Join(root, backupBundleSnapshotPath)
	if err := trie.SaveSnapshotWithJournalSequence(snapshotPath, 1); err != nil {
		t.Fatalf("SaveSnapshotWithJournalSequence() error = %v", err)
	}
	file, err := backupBundleFileInfo(backupBundleSnapshotPath, snapshotPath)
	if err != nil {
		t.Fatalf("backupBundleFileInfo() error = %v", err)
	}
	manifest := BackupBundleManifest{
		Version:         BackupBundleVersion,
		Mode:            BackupModeSnapshot,
		Snapshot:        backupBundleSnapshotPath,
		JournalSequence: 2,
		Files:           []BackupBundleFile{file},
	}
	manifest.Consistency, err = hatBackup.BuildBundleConsistency(manifest)
	if err != nil {
		t.Fatalf("BuildBundleConsistency() error = %v", err)
	}
	if _, err := verifySnapshotBackupRoot(snapshotPath, "test", manifest, root); err == nil {
		t.Fatal("verifySnapshotBackupRoot() error = nil, want snapshot boundary rejection")
	}
}

func TestCHU50BackupConsistencyBindsIncrementalRepository(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v, want ok", response)
	}
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()

	repository := filepath.Join(t.TempDir(), "repository")
	manifest, err := CreateBackupBundle(repository, trie, journal, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle(incremental) error = %v", err)
	}
	if manifest.Consistency == nil || manifest.Consistency.PartSequence != 1 || manifest.Consistency.JournalSequence != 1 || manifest.Consistency.Journal == nil {
		t.Fatalf("incremental consistency = %#v, want part/journal sequence 1", manifest.Consistency)
	}
	if len(manifest.Consistency.Parts) == 0 {
		t.Fatal("incremental consistency parts = empty, want checkpoint parts")
	}
	stored, err := readBackupRepositoryManifest(repository, "")
	if err != nil {
		t.Fatalf("readBackupRepositoryManifest() error = %v", err)
	}
	if err := hatBackup.ValidateBundleConsistency(stored); err != nil {
		t.Fatalf("ValidateBundleConsistency(stored incremental) error = %v", err)
	}
}

func TestCHU50LegacyBackupManifestWithoutConsistencyRemainsValid(t *testing.T) {
	manifest := hatBackup.BundleManifest{
		Version: hatBackup.BundleVersion,
		Mode:    hatBackup.ModeSnapshot,
	}
	if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
		t.Fatalf("ValidateBundleConsistency(legacy) error = %v, want nil", err)
	}
}

func bundleFileByPath(t *testing.T, manifest BackupBundleManifest, path string) hatBackup.BundleFile {
	t.Helper()
	for _, file := range manifest.Files {
		if file.Path == path {
			return file
		}
	}
	t.Fatalf("manifest files = %#v, missing %s", manifest.Files, path)
	return hatBackup.BundleFile{}
}
