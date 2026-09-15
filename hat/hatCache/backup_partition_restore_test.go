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

func TestRestoreBackupBundleRejectsPartitionSubsetWithJournal(t *testing.T) {
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

	bundlePath := filepath.Join(t.TempDir(), "partitioned-with-journal.tar.gz")
	_, err = CreateBackupBundle(bundlePath, source, journal, BackupBundleOptions{
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
	_, err = RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{Partition: selector})
	if err == nil || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("RestoreBackupBundle(partition subset with journal) error = %v, want journal rejection", err)
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
