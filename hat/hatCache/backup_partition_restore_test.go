package hatCache

import (
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestValidatePartitionRestoreSelectionRejectsMismatchedCoveragePair(t *testing.T) {
	selected, err := hatBackup.ValidatePartitionRestoreSelection(hatBackup.BundleManifest{
		Partition: &hatBackup.PartitionMetadata{
			Local:       true,
			Partitions:  []string{"sg", "us"},
			KeyPrefixes: []string{"region:sg/", "region:us/"},
		},
	}, &hatBackup.PartitionMetadata{
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:us/"},
	})
	if selected || err == nil || !strings.Contains(err.Error(), "partition key prefixes") {
		t.Fatalf("ValidatePartitionRestoreSelection() = %t/%v, want coverage-pair rejection", selected, err)
	}
}

func TestValidatePartitionRestoreSelectionRejectsEmptyBackupMetadata(t *testing.T) {
	selected, err := hatBackup.ValidatePartitionRestoreSelection(hatBackup.BundleManifest{
		Partition: &hatBackup.PartitionMetadata{Local: true},
	}, &hatBackup.PartitionMetadata{Partitions: []string{"sg"}})
	if selected || err == nil || !strings.Contains(err.Error(), "invalid backup metadata") {
		t.Fatalf("ValidatePartitionRestoreSelection() = %t/%v, want empty-metadata rejection", selected, err)
	}
}

func TestValidatePartitionRestoreJournalRejectsReplayEntry(t *testing.T) {
	root := t.TempDir()
	source := CreateHatTrie()
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(root, backupBundleJournalPath))
	if err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "region:sg/user:1",
		Value:   "Singapore",
	}); !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	err = validatePartitionRestoreJournal(root, BackupBundleManifest{
		Journal:         backupBundleJournalPath,
		JournalFormat:   string(DefaultCommandJournalFormat),
		JournalSequence: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "checkpoint-only") {
		t.Fatalf("validatePartitionRestoreJournal() error = %v, want replay-entry rejection", err)
	}
}

func TestRestoreBackupBundleAllowsCheckpointJournalForPartitionSubset(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for _, request := range []CacheCommandRequest{
		{Command: "SETSTR", Key: "region:sg/user:1", Value: "Singapore"},
		{Command: "SETSTR", Key: "region:us/user:1", Value: "United States"},
	} {
		if response := journal.ExecuteCommand(source, request); !response.OK {
			t.Fatalf("ExecuteCommand(%#v) = %#v, want ok", request, response)
		}
	}

	bundlePath := filepath.Join(t.TempDir(), "partitioned-with-checkpoint-journal.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, source, journal, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
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

	selector := &BackupPartitionMetadata{
		Mode:        "partitioned",
		Local:       true,
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:sg/"},
	}
	report, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{Partition: selector})
	if err != nil {
		t.Fatalf("RestoreBackupBundle(partition subset with checkpoint journal) error = %v", err)
	}
	if report.Journal == "" {
		t.Fatal("RestoreBackupBundle() omitted the checkpoint journal path")
	}
	entries, err := readCommandJournalEntries(report.Journal)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 1 || !entries[0].Checkpoint || entries[0].Sequence != manifest.JournalSequence {
		t.Fatalf("restored journal entries = %#v, want one checkpoint at sequence %d", entries, manifest.JournalSequence)
	}
	restored := newTestTrie(t)
	defer restored.Destroy()
	if err := restored.LoadSnapshot(report.Snapshot); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if !restored.Exists("region:sg/user:1") || restored.Exists("region:us/user:1") {
		t.Fatal("selective restore did not preserve only the selected partition")
	}
}

func TestRestoreBackupBundleRejectsPartitionSubsetForPebbleCheckpoint(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("region:sg/user:1", "Singapore")
	source.UpsertString("region:us/user:1", "United States")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	bundlePath := filepath.Join(t.TempDir(), "partitioned-checkpoint.tar.gz")
	_, err = CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
		Partition: BackupPartitionMetadata{
			Mode:        "partitioned",
			Local:       true,
			Partitions:  []string{"sg", "us"},
			KeyPrefixes: []string{"region:sg/", "region:us/"},
		},
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	selector := &BackupPartitionMetadata{
		Mode:        "partitioned",
		Local:       true,
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:sg/"},
	}
	_, err = RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{Partition: selector})
	if err == nil || !strings.Contains(err.Error(), "snapshot backup") {
		t.Fatalf("RestoreBackupBundle(partition subset checkpoint) error = %v, want snapshot-only rejection", err)
	}
}

func TestRehearseRestoreRejectsPartitionSubset(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("region:sg/user:1", "Singapore")
	source.UpsertString("region:us/user:1", "United States")
	bundlePath := filepath.Join(t.TempDir(), "partitioned.tar.gz")
	_, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
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

	_, err = RehearseRestore(bundlePath, RestoreRehearsalOptions{Partition: &BackupPartitionMetadata{
		Mode:        "partitioned",
		Local:       true,
		Partitions:  []string{"sg"},
		KeyPrefixes: []string{"region:sg/"},
	}})
	if err == nil || !strings.Contains(err.Error(), "restore rehearsal does not support selective partition restore") {
		t.Fatalf("RehearseRestore(partition subset) error = %v, want explicit rejection", err)
	}
}
