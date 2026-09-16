package hatCache

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatBackup"
	"hatrie_cache/internal/jsonwire"
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

func TestSelectivePartitionJournalRejectsComplexCommand(t *testing.T) {
	_, err := selectivePartitionJournalRequestKey(commandJournalEntry{Request: CacheCommandRequest{
		Command: "BATCH",
		Batch:   []CacheCommandRequest{{Command: "SETSTR", Key: "region:sg/user:1", Value: "Singapore"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "cannot safely filter") {
		t.Fatalf("selectivePartitionJournalRequestKey() error = %v, want complex-command rejection", err)
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

func TestRestoreBackupBundleFiltersReplayTailForPartitionSubset(t *testing.T) {
	for _, journalFormat := range []CommandJournalFormat{CommandJournalFormatJSON, CommandJournalFormatBinary} {
		t.Run(string(journalFormat), func(t *testing.T) {
			source := CreateHatTrie()
			defer source.Destroy()
			journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), backupBundleJournalPath), journalFormat)
			if err != nil {
				t.Fatal(err)
			}
			for _, request := range []CacheCommandRequest{
				{Command: "SETSTR", Key: "region:sg/base", Value: "before"},
				{Command: "SETSTR", Key: "region:us/base", Value: "before"},
			} {
				if response := journal.ExecuteCommand(source, request); !response.OK {
					t.Fatalf("pre-snapshot ExecuteCommand(%#v) = %#v, want ok", request, response)
				}
			}

			baseBundlePath := filepath.Join(t.TempDir(), "partitioned-base.tar.gz")
			_, err = CreateBackupBundle(baseBundlePath, source, journal, BackupBundleOptions{
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
			for _, request := range []CacheCommandRequest{
				{Command: "SETSTR", Key: "region:sg/base", Value: "after"},
				{Command: "SETINT", Key: "region:sg/count", Value: "7"},
				{Command: "INC", Key: "region:sg/count", Value: "1"},
				{Command: "SETSTR", Key: "region:us/after", Value: "excluded"},
				{Command: "DEL", Key: "region:us/base"},
			} {
				if response := journal.ExecuteCommand(source, request); !response.OK {
					t.Fatalf("tail ExecuteCommand(%#v) = %#v, want ok", request, response)
				}
			}
			journalPath := journal.path
			if err := journal.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			tailBundlePath := rebuildBackupBundleWithJournal(t, baseBundlePath, journalPath)

			selector := &BackupPartitionMetadata{
				Mode:        "partitioned",
				Local:       true,
				Partitions:  []string{"sg"},
				KeyPrefixes: []string{"region:sg/"},
			}
			report, err := RestoreBackupBundle(tailBundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{Partition: selector})
			if err != nil {
				t.Fatalf("RestoreBackupBundle(partition subset with replay tail) error = %v", err)
			}
			if report.JournalSequence != 7 || report.RecoveredKeys != 2 {
				t.Fatalf("restore report sequence/keys = %d/%d, want 7/2", report.JournalSequence, report.RecoveredKeys)
			}
			entries, err := readCommandJournalEntries(report.Journal)
			if err != nil {
				t.Fatalf("readCommandJournalEntries() error = %v", err)
			}
			if len(entries) != 1 || !entries[0].Checkpoint || entries[0].Sequence != 7 {
				t.Fatalf("restored journal entries = %#v, want one checkpoint at sequence 7", entries)
			}
			restored := newTestTrie(t)
			defer restored.Destroy()
			if err := restored.LoadSnapshot(report.Snapshot); err != nil {
				t.Fatalf("LoadSnapshot() error = %v", err)
			}
			if restored.GetString("region:sg/base") != "after" || restored.GetCounter("region:sg/count") != 8 {
				t.Fatalf("selected replayed state = %q/%d, want after/8", restored.GetString("region:sg/base"), restored.GetCounter("region:sg/count"))
			}
			if restored.Exists("region:us/base") || restored.Exists("region:us/after") {
				t.Fatal("unselected replay tail data was restored")
			}
		})
	}
}

func rebuildBackupBundleWithJournal(t testing.TB, bundlePath string, journalPath string) string {
	t.Helper()
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		t.Fatalf("readBackupBundleManifest() error = %v", err)
	}
	root := t.TempDir()
	if err := extractBackupBundleFiles(bundlePath, root, manifest.Files); err != nil {
		t.Fatalf("extractBackupBundleFiles() error = %v", err)
	}
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile(journal) error = %v", err)
	}
	journalSequence := uint64(0)
	if _, err := scanCommandJournalEntries(journalPath, func(entry commandJournalEntry) error {
		if entry.Sequence > journalSequence {
			journalSequence = entry.Sequence
		}
		return nil
	}); err != nil {
		t.Fatalf("scanCommandJournalEntries() error = %v", err)
	}
	manifest.JournalSequence = journalSequence
	stagedJournalPath := filepath.Join(root, backupBundleJournalPath)
	if err := os.WriteFile(stagedJournalPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile(journal) error = %v", err)
	}
	for index := range manifest.Files {
		if manifest.Files[index].Path != backupBundleJournalPath {
			continue
		}
		manifest.Files[index], err = backupBundleFileInfo(backupBundleJournalPath, stagedJournalPath)
		if err != nil {
			t.Fatalf("backupBundleFileInfo(journal) error = %v", err)
		}
	}
	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		t.Fatalf("jsonwire.Marshal(manifest) error = %v", err)
	}
	manifestData = append(manifestData, '\n')
	output := filepath.Join(t.TempDir(), "partitioned-with-replay-tail.tar.gz")
	payloads := make([]backupBundlePayloadFile, 0, len(manifest.Files))
	for _, file := range manifest.Files {
		payloads = append(payloads, backupBundlePayloadFile{
			name: file.Path,
			path: filepath.Join(root, filepath.FromSlash(file.Path)),
		})
	}
	if err := writeFileAtomicStream(output, func(writer io.Writer) error {
		return writeBackupBundlePayloadTarGzip(writer, payloads, manifestData, manifest.CreatedAt)
	}); err != nil {
		t.Fatalf("writeBackupBundlePayloadTarGzip() error = %v", err)
	}
	return output
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
