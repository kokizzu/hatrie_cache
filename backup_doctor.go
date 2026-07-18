package hatriecache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	json "github.com/goccy/go-json"
)

type BackupDoctorReport struct {
	OK                  bool                       `json:"ok"`
	Kind                string                     `json:"kind"`
	Path                string                     `json:"path"`
	Snapshot            *BackupDoctorSnapshot      `json:"snapshot,omitempty"`
	Journal             *BackupDoctorJournal       `json:"journal,omitempty"`
	LevelDB             *BackupDoctorLevelDB       `json:"leveldb,omitempty"`
	Files               []BackupBundleFile         `json:"files,omitempty"`
	Partition           *BackupPartitionMetadata   `json:"partition,omitempty"`
	PartitionValidation *BackupPartitionValidation `json:"partition_validation,omitempty"`
	RecoveredKeys       int                        `json:"recovered_keys,omitempty"`
	JournalSequence     uint64                     `json:"journal_sequence,omitempty"`
}

type BackupDoctorSnapshot struct {
	Path            string `json:"path"`
	OK              bool   `json:"ok"`
	Keys            int    `json:"keys"`
	JournalSequence uint64 `json:"journal_sequence"`
}

type BackupDoctorJournal struct {
	Path         string `json:"path"`
	OK           bool   `json:"ok"`
	Entries      int    `json:"entries"`
	LastSequence uint64 `json:"last_sequence"`
}

type BackupDoctorLevelDB struct {
	Path string `json:"path"`
	OK   bool   `json:"ok"`
	Keys int    `json:"keys"`
}

type BackupPartitionValidation struct {
	OK                bool     `json:"ok"`
	CheckedKeys       int      `json:"checked_keys"`
	InvalidKeys       int      `json:"invalid_keys"`
	KeyPrefixes       []string `json:"key_prefixes,omitempty"`
	InvalidKeySamples []string `json:"invalid_key_samples,omitempty"`
}

const backupPartitionInvalidKeySampleLimit = 8

func VerifyBackupPath(path string) (BackupDoctorReport, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return BackupDoctorReport{}, errors.New("hatriecache: backup path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	if info.IsDir() {
		return VerifyBackupDirectory(path)
	}
	return VerifyBackupBundle(path)
}

func VerifyBackupBundle(path string) (BackupDoctorReport, error) {
	files, err := readBackupBundle(path)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	manifestData, ok := files[backupBundleManifestPath]
	if !ok {
		return BackupDoctorReport{}, errors.New("hatriecache: backup bundle missing manifest.json")
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return BackupDoctorReport{}, err
	}
	if manifest.Version != BackupBundleVersion {
		return BackupDoctorReport{}, fmt.Errorf("hatriecache: unsupported backup bundle version %d", manifest.Version)
	}
	for _, file := range manifest.Files {
		data, ok := files[file.Path]
		if !ok {
			return BackupDoctorReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", file.Path)
		}
		if err := verifyBackupFileChecksum(file, data); err != nil {
			return BackupDoctorReport{}, err
		}
	}

	report := BackupDoctorReport{
		OK:              true,
		Kind:            "bundle",
		Path:            path,
		Files:           manifest.Files,
		Partition:       cloneBackupPartitionMetadata(manifest.Partition),
		JournalSequence: manifest.JournalSequence,
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	if manifest.Snapshot == "" {
		return BackupDoctorReport{}, errors.New("hatriecache: backup bundle manifest missing snapshot")
	}
	snapshotData, ok := files[manifest.Snapshot]
	if !ok {
		return BackupDoctorReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", manifest.Snapshot)
	}
	snapshotReport, err := verifySnapshotBytes(trie, manifest.Snapshot, snapshotData)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	report.Snapshot = &snapshotReport
	report.RecoveredKeys = trie.Size()
	partitionValidation, err := validateBackupPartitionMetadataAgainstTrie(trie, manifest.Partition)
	if partitionValidation != nil {
		report.PartitionValidation = partitionValidation
	}
	if err != nil {
		report.OK = false
		return report, err
	}

	if manifest.Journal != "" {
		journalData, ok := files[manifest.Journal]
		if !ok {
			return BackupDoctorReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", manifest.Journal)
		}
		journalReport, err := verifyJournalBytes(manifest.Journal, journalData)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		report.Journal = &journalReport
	}
	return report, nil
}

func VerifyBackupDirectory(path string) (BackupDoctorReport, error) {
	report := BackupDoctorReport{OK: true, Kind: "directory", Path: path}
	trie := CreateHatTrie()
	defer trie.Destroy()

	snapshotPath := firstExistingPath(filepath.Join(path, "snapshot.hc"), filepath.Join(path, "snapshot.json"))
	var snapshotMetadata SnapshotMetadata
	if snapshotPath != "" {
		metadata, err := trie.LoadSnapshotWithMetadata(snapshotPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		snapshotMetadata = metadata
		report.Snapshot = &BackupDoctorSnapshot{
			Path:            snapshotPath,
			OK:              true,
			Keys:            trie.Size(),
			JournalSequence: metadata.JournalSequence,
		}
		report.JournalSequence = metadata.JournalSequence
	}

	journalPath := filepath.Join(path, "commands.journal")
	if fileExists(journalPath) {
		journal, err := OpenCommandJournal(journalPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		defer journal.Close()
		if _, err := journal.Replay(trie, snapshotMetadata.JournalSequence); err != nil {
			return BackupDoctorReport{}, err
		}
		report.Journal = &BackupDoctorJournal{
			Path:         journalPath,
			OK:           true,
			Entries:      countJournalEntries(journalPath),
			LastSequence: journal.Sequence(),
		}
		report.JournalSequence = journal.Sequence()
	}

	levelDBPath := filepath.Join(path, "cache.leveldb")
	if fileExists(levelDBPath) {
		store, err := OpenLevelDBStore(levelDBPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		defer store.Close()
		loaded := CreateHatTrie()
		defer loaded.Destroy()
		count, err := store.Load(loaded)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		report.LevelDB = &BackupDoctorLevelDB{Path: levelDBPath, OK: true, Keys: count}
		if report.RecoveredKeys == 0 {
			report.RecoveredKeys = loaded.Size()
		}
	}

	if report.Snapshot == nil && report.Journal == nil && report.LevelDB == nil {
		return BackupDoctorReport{}, errors.New("hatriecache: backup directory contains no recognized snapshot, journal, or LevelDB data")
	}
	if report.RecoveredKeys == 0 {
		report.RecoveredKeys = trie.Size()
	}
	return report, nil
}

func readBackupBundle(path string) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	files := map[string][]byte{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tarReader)
		if err != nil {
			return nil, err
		}
		files[header.Name] = data
	}
	return files, nil
}

func verifyBackupFileChecksum(file BackupBundleFile, data []byte) error {
	sum := sha256.Sum256(data)
	if file.Size != int64(len(data)) || file.SHA256 != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("hatriecache: backup file checksum mismatch for %s", file.Path)
	}
	return nil
}

func validateBackupPartitionMetadataAgainstTrie(trie *HatTrie, partition *BackupPartitionMetadata) (*BackupPartitionValidation, error) {
	if trie == nil || partition == nil || len(partition.KeyPrefixes) == 0 {
		return nil, nil
	}
	keys, err := trie.KeysWithPrefixChecked("", true)
	if err != nil {
		return nil, err
	}
	validation := &BackupPartitionValidation{
		OK:          true,
		CheckedKeys: len(keys),
		KeyPrefixes: append([]string(nil), partition.KeyPrefixes...),
	}
	for _, key := range keys {
		if backupPartitionKeyCoveredByPrefix(key, partition.KeyPrefixes) {
			continue
		}
		validation.OK = false
		validation.InvalidKeys++
		if len(validation.InvalidKeySamples) < backupPartitionInvalidKeySampleLimit {
			validation.InvalidKeySamples = append(validation.InvalidKeySamples, key)
		}
	}
	if validation.InvalidKeys == 0 {
		return validation, nil
	}
	return validation, fmt.Errorf("hatriecache: backup partition metadata does not cover key %q", validation.InvalidKeySamples[0])
}

func backupPartitionKeyCoveredByPrefix(key string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func cloneBackupPartitionValidation(input *BackupPartitionValidation) *BackupPartitionValidation {
	if input == nil {
		return nil
	}
	out := *input
	out.KeyPrefixes = append([]string(nil), input.KeyPrefixes...)
	out.InvalidKeySamples = append([]string(nil), input.InvalidKeySamples...)
	return &out
}

func verifySnapshotBytes(trie *HatTrie, name string, data []byte) (BackupDoctorSnapshot, error) {
	path := filepath.Join(os.TempDir(), "hatrie-cache-doctor-snapshot-*")
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		return BackupDoctorSnapshot{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return BackupDoctorSnapshot{}, err
	}
	if err := tmp.Close(); err != nil {
		return BackupDoctorSnapshot{}, err
	}
	metadata, err := trie.LoadSnapshotWithMetadata(tmpPath)
	if err != nil {
		return BackupDoctorSnapshot{}, err
	}
	return BackupDoctorSnapshot{Path: name, OK: true, Keys: trie.Size(), JournalSequence: metadata.JournalSequence}, nil
}

func verifyJournalBytes(name string, data []byte) (BackupDoctorJournal, error) {
	tmp, err := os.CreateTemp("", "hatrie-cache-doctor-journal-*")
	if err != nil {
		return BackupDoctorJournal{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return BackupDoctorJournal{}, err
	}
	if err := tmp.Close(); err != nil {
		return BackupDoctorJournal{}, err
	}
	return verifyJournalFile(name, tmpPath)
}

func verifyJournalFile(displayPath string, path string) (BackupDoctorJournal, error) {
	entries, err := scanBackupJournalEntries(path)
	if err != nil {
		return BackupDoctorJournal{}, err
	}
	var last uint64
	for _, entry := range entries {
		if entry.Sequence > last {
			last = entry.Sequence
		}
	}
	return BackupDoctorJournal{Path: displayPath, OK: true, Entries: len(entries), LastSequence: last}, nil
}

func scanBackupJournalEntries(path string) ([]commandJournalEntry, error) {
	var entries []commandJournalEntry
	_, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func countJournalEntries(path string) int {
	entries, err := scanBackupJournalEntries(path)
	if err != nil {
		return 0
	}
	return len(entries)
}

func firstExistingPath(paths ...string) string {
	for _, path := range paths {
		if fileExists(path) {
			return path
		}
	}
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
