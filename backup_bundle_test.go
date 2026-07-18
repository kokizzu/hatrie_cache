package hatriecache

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCreateBackupBundleIncludesManifestSnapshotAndJournalCheckpoint(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()

	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	createdAt := time.Date(2026, 7, 16, 10, 30, 0, 0, time.UTC)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, ht, journal, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
		CreatedAt:      createdAt,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if manifest.JournalSequence != 1 || manifest.SnapshotFormat != string(SnapshotFormatJSON) || manifest.JournalFormat != string(CommandJournalFormatJSON) {
		t.Fatalf("manifest = %#v, want sequence 1 JSON snapshot and journal", manifest)
	}

	files := readBackupBundleFiles(t, bundlePath)
	for _, name := range []string{backupBundleManifestPath, backupBundleSnapshotPath, backupBundleJournalPath} {
		if len(files[name]) == 0 && name != backupBundleJournalPath {
			t.Fatalf("bundle missing %s", name)
		}
		if _, ok := files[name]; !ok {
			t.Fatalf("bundle missing %s", name)
		}
	}

	var bundledManifest BackupBundleManifest
	if err := json.Unmarshal(files[backupBundleManifestPath], &bundledManifest); err != nil {
		t.Fatalf("Unmarshal(manifest.json) error = %v", err)
	}
	if bundledManifest.JournalSequence != manifest.JournalSequence || bundledManifest.CreatedAt != createdAt {
		t.Fatalf("bundled manifest = %#v, want returned manifest %#v", bundledManifest, manifest)
	}
	assertBundleFileChecksum(t, bundledManifest, backupBundleSnapshotPath, files[backupBundleSnapshotPath])
	assertBundleFileChecksum(t, bundledManifest, backupBundleJournalPath, files[backupBundleJournalPath])

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := os.WriteFile(snapshotPath, files[backupBundleSnapshotPath], 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	restored := newTestTrie(t)
	if _, err := restored.LoadSnapshotWithMetadata(snapshotPath); err != nil {
		t.Fatalf("LoadSnapshotWithMetadata(bundle snapshot) error = %v", err)
	}
	if got := restored.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("restored GET response = %#v, want ivi", got)
	}

	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	if err := os.WriteFile(journalPath, files[backupBundleJournalPath], 0o600); err != nil {
		t.Fatalf("WriteFile(journal) error = %v", err)
	}
	entries, err := readCommandJournalEntries(journalPath)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(bundle journal) error = %v", err)
	}
	if len(entries) != 1 || !entries[0].Checkpoint || entries[0].Sequence != 1 {
		t.Fatalf("bundle journal entries = %#v, want checkpoint at sequence 1", entries)
	}
}

func TestCreateBackupBundleRejectsMissingPath(t *testing.T) {
	_, err := CreateBackupBundle("", newTestTrie(t), nil, BackupBundleOptions{})
	if err == nil {
		t.Fatal("CreateBackupBundle(empty path) error = nil, want rejection")
	}
}

func TestCreateBackupBundleCarriesPartitionMetadataToDoctorAndRestore(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "sg:session:1", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	partition := BackupPartitionMetadata{
		Mode:                "partitioned",
		Partitions:          []string{"sg"},
		NodeID:              "node-sg-a",
		TopologyEpoch:       42,
		TopologyFingerprint: "topology-v1",
		KeyPrefixes:         []string{"sg:"},
	}
	manifest, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
		Partition:      partition,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	if manifest.Partition == nil || !reflect.DeepEqual(*manifest.Partition, partition) {
		t.Fatalf("manifest partition = %#v, want %#v", manifest.Partition, partition)
	}

	files := readBackupBundleFiles(t, bundlePath)
	var bundledManifest BackupBundleManifest
	if err := json.Unmarshal(files[backupBundleManifestPath], &bundledManifest); err != nil {
		t.Fatalf("Unmarshal(manifest.json) error = %v", err)
	}
	if bundledManifest.Partition == nil || !reflect.DeepEqual(*bundledManifest.Partition, partition) {
		t.Fatalf("bundled manifest partition = %#v, want %#v", bundledManifest.Partition, partition)
	}

	doctor, err := VerifyBackupBundle(bundlePath)
	if err != nil {
		t.Fatalf("VerifyBackupBundle() error = %v", err)
	}
	if doctor.Partition == nil || !reflect.DeepEqual(*doctor.Partition, partition) {
		t.Fatalf("doctor partition = %#v, want %#v", doctor.Partition, partition)
	}
	if doctor.PartitionValidation == nil || !doctor.PartitionValidation.OK || doctor.PartitionValidation.CheckedKeys != 1 || doctor.PartitionValidation.InvalidKeys != 0 {
		t.Fatalf("doctor partition validation = %#v, want ok with one checked key", doctor.PartitionValidation)
	}

	restoreReport, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "data"), BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreBackupBundle() error = %v", err)
	}
	if restoreReport.Partition == nil || !reflect.DeepEqual(*restoreReport.Partition, partition) {
		t.Fatalf("restore partition = %#v, want %#v", restoreReport.Partition, partition)
	}
	if restoreReport.PartitionValidation == nil || !restoreReport.PartitionValidation.OK || restoreReport.PartitionValidation.CheckedKeys != 1 || restoreReport.PartitionValidation.InvalidKeys != 0 {
		t.Fatalf("restore partition validation = %#v, want ok with one checked key", restoreReport.PartitionValidation)
	}
}

func TestVerifyBackupBundleRejectsPartitionPrefixMismatch(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "sg:session:1", Value: "ok"}); !got.OK {
		t.Fatalf("SETSTR(sg) response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "us:session:1", Value: "wrong partition"}); !got.OK {
		t.Fatalf("SETSTR(us) response = %#v, want ok", got)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	_, err := CreateBackupBundle(bundlePath, ht, nil, BackupBundleOptions{
		SnapshotFormat: SnapshotFormatJSON,
		Partition: BackupPartitionMetadata{
			Mode:        "partitioned",
			Partitions:  []string{"sg"},
			KeyPrefixes: []string{"sg:"},
		},
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	_, err = VerifyBackupBundle(bundlePath)
	if err == nil || !strings.Contains(err.Error(), "backup partition metadata does not cover key") {
		t.Fatalf("VerifyBackupBundle(partition mismatch) error = %v, want partition coverage error", err)
	}
}

func TestCreateBackupBundleRejectsInvalidPartitionMetadata(t *testing.T) {
	_, err := CreateBackupBundle(filepath.Join(t.TempDir(), "backup.tar.gz"), newTestTrie(t), nil, BackupBundleOptions{
		Partition: BackupPartitionMetadata{Mode: "partitioned", Partitions: []string{"sg", " "}},
	})
	if err == nil {
		t.Fatal("CreateBackupBundle(invalid partition) error = nil, want rejection")
	}
}

func readBackupBundleFiles(t *testing.T, path string) map[string][]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(bundle) error = %v", err)
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader(bundle gzip) error = %v", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	files := make(map[string][]byte)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next() error = %v", err)
		}
		data, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatalf("ReadAll(%s) error = %v", header.Name, err)
		}
		files[header.Name] = data
	}
	return files
}

func assertBundleFileChecksum(t *testing.T, manifest BackupBundleManifest, path string, data []byte) {
	t.Helper()
	for _, file := range manifest.Files {
		if file.Path != path {
			continue
		}
		sum := sha256.Sum256(data)
		if file.Size != int64(len(data)) || file.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("manifest file %s = %#v, want size/checksum for bundled data", path, file)
		}
		return
	}
	t.Fatalf("manifest missing file %s", path)
}
