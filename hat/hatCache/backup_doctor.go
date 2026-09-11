package hatCache

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"

	"hatrie_cache/hat/hatBackup"

	json "github.com/goccy/go-json"
)

type BackupDoctorReport = hatBackup.DoctorReport
type BackupDoctorSnapshot = hatBackup.DoctorSnapshot
type BackupDoctorJournal = hatBackup.DoctorJournal
type BackupDoctorLevelDB = hatBackup.DoctorStore
type BackupPartitionValidation = hatBackup.PartitionValidation

const backupPartitionInvalidKeySampleLimit = 8
const maxBackupBundleManifestBytes = 1 << 20

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
		if fileExists(filepath.Join(path, backupRepositoryDescriptorPath)) {
			return VerifyBackupRepository(path, "")
		}
		return VerifyBackupDirectory(path)
	}
	return VerifyBackupBundle(path)
}

func VerifyBackupBundle(path string) (BackupDoctorReport, error) {
	manifest, err := readBackupBundleManifest(path)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	if backupBundleManifestMode(manifest) == BackupModePebbleCheckpoint {
		return verifyPebbleCheckpointBundle(path, manifest)
	}
	return verifySnapshotBackupBundle(path, manifest)
}

func verifySnapshotBackupBundle(path string, manifest BackupBundleManifest) (BackupDoctorReport, error) {
	if manifest.Snapshot == "" {
		return BackupDoctorReport{}, errors.New("hatriecache: backup bundle manifest missing snapshot")
	}
	tmpDir, err := os.MkdirTemp(filepath.Dir(path), filepath.Base(path)+".verify-*")
	if err != nil {
		return BackupDoctorReport{}, err
	}
	defer os.RemoveAll(tmpDir)
	if err := extractBackupBundleFiles(path, tmpDir, manifest.Files); err != nil {
		return BackupDoctorReport{}, err
	}
	return verifySnapshotBackupRoot(path, "bundle", manifest, tmpDir)
}

func verifySnapshotBackupRoot(displayPath string, kind string, manifest BackupBundleManifest, root string) (BackupDoctorReport, error) {
	if manifest.Snapshot == "" {
		return BackupDoctorReport{}, errors.New("hatriecache: backup manifest missing snapshot")
	}
	report := BackupDoctorReport{
		OK:              true,
		Kind:            kind,
		Path:            displayPath,
		Files:           manifest.Files,
		Partition:       cloneBackupPartitionMetadata(manifest.Partition),
		JournalSequence: manifest.JournalSequence,
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	snapshotPath := filepath.Join(root, filepath.FromSlash(manifest.Snapshot))
	metadata, err := trie.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	report.Snapshot = &BackupDoctorSnapshot{Path: manifest.Snapshot, OK: true, Keys: trie.Size(), JournalSequence: metadata.JournalSequence}
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
		journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
		journalReport, entries, err := verifyJournalFileWithEntries(manifest.Journal, journalPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		report.Journal = &journalReport
		partitionValidation, err := validateBackupPartitionMetadataAgainstJournalEntries(report.PartitionValidation, entries, manifest.Partition)
		if partitionValidation != nil {
			report.PartitionValidation = partitionValidation
		}
		if err != nil {
			report.OK = false
			return report, err
		}
		journalFormat := DefaultCommandJournalFormat
		if manifest.JournalFormat != "" {
			journalFormat, err = ParseCommandJournalFormat(manifest.JournalFormat)
			if err != nil {
				return BackupDoctorReport{}, err
			}
		}
		journal, err := OpenCommandJournalWithFormat(journalPath, journalFormat)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		if _, err := journal.Replay(trie, metadata.JournalSequence); err != nil {
			_ = journal.Close()
			return BackupDoctorReport{}, err
		}
		report.RecoveredKeys = trie.Size()
		report.JournalSequence = journal.Sequence()
		if err := journal.Close(); err != nil {
			return BackupDoctorReport{}, err
		}
	}
	report.StateChecksum, err = backupStateChecksum(trie)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	return report, nil
}

func backupBundleManifestMode(manifest BackupBundleManifest) BackupMode {
	if manifest.Mode == "" {
		return BackupModeSnapshot
	}
	return manifest.Mode
}

func readBackupBundleManifest(bundlePath string) (BackupBundleManifest, error) {
	file, err := os.Open(bundlePath)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return BackupBundleManifest{}, errors.New("hatriecache: backup bundle missing manifest.json")
		}
		if err != nil {
			return BackupBundleManifest{}, err
		}
		if header.Name != backupBundleManifestPath || header.Typeflag != tar.TypeReg {
			continue
		}
		if header.Size < 0 || header.Size > maxBackupBundleManifestBytes {
			return BackupBundleManifest{}, errors.New("hatriecache: backup bundle manifest is too large")
		}
		data, err := io.ReadAll(io.LimitReader(tarReader, maxBackupBundleManifestBytes+1))
		if err != nil {
			return BackupBundleManifest{}, err
		}
		var manifest BackupBundleManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return BackupBundleManifest{}, err
		}
		if manifest.Version != BackupBundleVersion {
			return BackupBundleManifest{}, fmt.Errorf("hatriecache: unsupported backup bundle version %d", manifest.Version)
		}
		return manifest, nil
	}
}

func verifyPebbleCheckpointBundle(bundlePath string, manifest BackupBundleManifest) (BackupDoctorReport, error) {
	if manifest.Store != backupBundleStorePath || manifest.StorageBackend != string(StorageBackendPebble) {
		return BackupDoctorReport{}, errors.New("hatriecache: invalid Pebble checkpoint manifest")
	}
	tmpDir, err := os.MkdirTemp(filepath.Dir(bundlePath), filepath.Base(bundlePath)+".verify-*")
	if err != nil {
		return BackupDoctorReport{}, err
	}
	defer os.RemoveAll(tmpDir)
	if err := extractBackupBundleFiles(bundlePath, tmpDir, manifest.Files); err != nil {
		return BackupDoctorReport{}, err
	}
	return verifyPebbleBackupRoot(bundlePath, "bundle", manifest, tmpDir)
}

func VerifyBackupRepository(path string, backupID string) (BackupDoctorReport, error) {
	if err := verifyBackupRepositoryDescriptor(path); err != nil {
		return BackupDoctorReport{}, err
	}
	manifest, err := readBackupRepositoryManifest(path, backupID)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	tmpDir, err := os.MkdirTemp(path, ".verify-*")
	if err != nil {
		return BackupDoctorReport{}, err
	}
	defer os.RemoveAll(tmpDir)
	if _, err := materializeBackupRepository(path, manifest.BackupID, tmpDir); err != nil {
		return BackupDoctorReport{}, err
	}
	return verifyPebbleBackupRoot(path, "repository", manifest, tmpDir)
}

func verifyPebbleBackupRoot(displayPath string, kind string, manifest BackupBundleManifest, root string) (BackupDoctorReport, error) {
	if manifest.Store != backupBundleStorePath || manifest.StorageBackend != string(StorageBackendPebble) {
		return BackupDoctorReport{}, errors.New("hatriecache: invalid Pebble backup manifest")
	}
	format, err := ParseStorageFormat(manifest.StorageFormat)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	storePath := filepath.Join(root, filepath.FromSlash(manifest.Store))
	store, err := openPebbleStoreReadOnlyWithFormat(storePath, format)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	defer store.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	count, err := store.Load(trie)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	report := BackupDoctorReport{
		OK:              true,
		Kind:            kind,
		Path:            displayPath,
		BackupID:        manifest.BackupID,
		LevelDB:         &BackupDoctorLevelDB{Path: manifest.Store, Backend: manifest.StorageBackend, OK: true, Keys: count},
		Files:           manifest.Files,
		Partition:       cloneBackupPartitionMetadata(manifest.Partition),
		RecoveredKeys:   trie.Size(),
		JournalSequence: manifest.JournalSequence,
	}
	partitionValidation, err := validateBackupPartitionMetadataAgainstTrie(trie, manifest.Partition)
	if partitionValidation != nil {
		report.PartitionValidation = partitionValidation
	}
	if err != nil {
		report.OK = false
		return report, err
	}
	report.StateChecksum, err = backupStateChecksum(trie)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	if manifest.Journal != "" {
		journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
		journalReport, entries, err := verifyJournalFileWithEntries(manifest.Journal, journalPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		report.Journal = &journalReport
		partitionValidation, err = validateBackupPartitionMetadataAgainstJournalEntries(report.PartitionValidation, entries, manifest.Partition)
		if partitionValidation != nil {
			report.PartitionValidation = partitionValidation
		}
		if err != nil {
			report.OK = false
			return report, err
		}
	}
	return report, nil
}

func extractBackupBundleFiles(bundlePath string, destination string, declared []BackupBundleFile) error {
	return extractBackupBundleFilesWithMode(bundlePath, destination, declared, false)
}

func extractBackupBundleFilesWithResume(bundlePath string, destination string, declared []BackupBundleFile) error {
	return extractBackupBundleFilesWithMode(bundlePath, destination, declared, true)
}

func extractBackupBundleFilesWithMode(bundlePath string, destination string, declared []BackupBundleFile, resume bool) error {
	expected := make(map[string]BackupBundleFile, len(declared))
	for _, file := range declared {
		clean, err := cleanBackupBundlePath(file.Path)
		if err != nil {
			return err
		}
		if clean == backupBundleManifestPath {
			return errors.New("hatriecache: backup manifest must not declare itself as a payload")
		}
		if _, exists := expected[clean]; exists {
			return fmt.Errorf("hatriecache: duplicate backup file declaration %s", clean)
		}
		expected[clean] = file
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return err
	}
	if resume {
		if err := pruneRestoreStaging(destination, expected); err != nil {
			return err
		}
	}
	archive, err := os.Open(bundlePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	gzipReader, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	seen := make(map[string]struct{}, len(expected))
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		clean, err := cleanBackupBundlePath(header.Name)
		if err != nil {
			return err
		}
		if clean == backupBundleManifestPath {
			continue
		}
		declaration, ok := expected[clean]
		if !ok {
			return fmt.Errorf("hatriecache: backup bundle contains undeclared file %s", clean)
		}
		if header.Typeflag != tar.TypeReg {
			return fmt.Errorf("hatriecache: backup bundle payload %s is not a regular file", clean)
		}
		if _, duplicate := seen[clean]; duplicate {
			return fmt.Errorf("hatriecache: backup bundle contains duplicate file %s", clean)
		}
		seen[clean] = struct{}{}
		target := filepath.Join(destination, filepath.FromSlash(clean))
		if resume {
			if err := hatBackup.RejectRestoreSymlinkComponents(filepath.Dir(target)); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		if err := extractBackupBundlePayload(tarReader, target, declaration, resume); err != nil {
			return err
		}
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("hatriecache: backup bundle missing %s", name)
		}
	}
	return nil
}

func extractBackupBundlePayload(reader io.Reader, target string, declaration BackupBundleFile, resume bool) error {
	if !resume {
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		hash := sha256.New()
		size, copyErr := io.Copy(io.MultiWriter(output, hash), reader)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return verifyBackupBundlePayload(declaration, size, hash.Sum(nil))
	}

	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore staging payload is not a regular file: %s", target)
		}
		if info.Size() == declaration.Size {
			existingHash, err := checksumRestoreFile(target)
			if err != nil {
				return err
			}
			if existingHash == declaration.SHA256 {
				hash := sha256.New()
				size, err := io.Copy(hash, reader)
				if err != nil {
					return err
				}
				return verifyBackupBundlePayload(declaration, size, hash.Sum(nil))
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	temporary, err := os.CreateTemp(filepath.Dir(target), "."+filepath.Base(target)+".restore-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(temporary, hash), reader)
	closeErr := temporary.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := verifyBackupBundlePayload(declaration, size, hash.Sum(nil)); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, target); err != nil {
		return err
	}
	removeTemporary = false
	return nil
}

func verifyBackupBundlePayload(declaration BackupBundleFile, size int64, digest []byte) error {
	if size != declaration.Size || hex.EncodeToString(digest) != declaration.SHA256 {
		return fmt.Errorf("hatriecache: backup file checksum mismatch for %s", declaration.Path)
	}
	return nil
}

func checksumRestoreFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func pruneRestoreStaging(destination string, expected map[string]BackupBundleFile) error {
	info, err := os.Lstat(destination)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("hatriecache: restore resume staging is not a directory: %s", destination)
	}
	var files []string
	var directories []string
	err = filepath.WalkDir(destination, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == destination {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore staging contains symlink: %s", path)
		}
		relative, err := filepath.Rel(destination, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(relative)
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		entryInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if !entryInfo.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore staging contains non-regular file: %s", path)
		}
		if _, ok := expected[name]; !ok {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range files {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	sort.Slice(directories, func(left, right int) bool {
		return len(directories[left]) > len(directories[right])
	})
	for _, directory := range directories {
		relative, err := filepath.Rel(destination, directory)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(relative)
		keep := false
		for expectedName := range expected {
			if strings.HasPrefix(expectedName, name+"/") {
				keep = true
				break
			}
		}
		if keep {
			continue
		}
		if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func cleanBackupBundlePath(name string) (string, error) {
	if strings.Contains(name, "\\") {
		return "", fmt.Errorf("hatriecache: invalid backup bundle path %q", name)
	}
	clean := pathpkg.Clean(name)
	if clean == "." || clean != name || strings.HasPrefix(clean, "/") || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("hatriecache: invalid backup bundle path %q", name)
	}
	return clean, nil
}

func VerifyBackupDirectory(path string) (BackupDoctorReport, error) {
	report := BackupDoctorReport{OK: true, Kind: "directory", Path: path}
	trie := CreateHatTrie()
	defer trie.Destroy()
	stateTrie := trie

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
		segments, err := listCommandJournalSegments(journalPath)
		if err != nil {
			return BackupDoctorReport{}, err
		}
		journalOptions := CommandJournalOptions{
			Format:              DefaultCommandJournalFormat,
			GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		}
		if len(segments) > 0 {
			journalOptions.SegmentMaxBytes = DefaultCommandJournalSegmentMaxBytes
			journalOptions.RetainedSegments = MaxCommandJournalRetainedSegments
		}
		journal, err := OpenCommandJournalWithOptions(journalPath, journalOptions)
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
			Entries:      countJournalEntries(journalPath, len(segments) > 0),
			LastSequence: journal.Sequence(),
		}
		report.JournalSequence = journal.Sequence()
	}

	levelDBPath := filepath.Join(path, "cache.leveldb")
	if fileExists(levelDBPath) {
		store, err := OpenPersistentStore(levelDBPath)
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
		report.LevelDB = &BackupDoctorLevelDB{Path: levelDBPath, Backend: string(store.Backend()), OK: true, Keys: count}
		if report.RecoveredKeys == 0 {
			report.RecoveredKeys = loaded.Size()
		}
		stateTrie = loaded
	}

	if report.Snapshot == nil && report.Journal == nil && report.LevelDB == nil {
		return BackupDoctorReport{}, errors.New("hatriecache: backup directory contains no recognized snapshot, journal, or persistent store data")
	}
	if report.RecoveredKeys == 0 {
		report.RecoveredKeys = trie.Size()
	}
	stateChecksum, err := backupStateChecksum(stateTrie)
	if err != nil {
		return BackupDoctorReport{}, err
	}
	report.StateChecksum = stateChecksum
	return report, nil
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

func validateBackupPartitionMetadataAgainstJournalEntries(validation *BackupPartitionValidation, entries []commandJournalEntry, partition *BackupPartitionMetadata) (*BackupPartitionValidation, error) {
	if partition == nil || len(partition.KeyPrefixes) == 0 {
		return validation, nil
	}
	if validation == nil {
		validation = &BackupPartitionValidation{
			OK:          true,
			KeyPrefixes: append([]string(nil), partition.KeyPrefixes...),
		}
	}
	for _, entry := range entries {
		for _, key := range backupPartitionJournalRequestKeys(entry.Request, nil) {
			validation.CheckedJournalKeys++
			if backupPartitionKeyCoveredByPrefix(key, partition.KeyPrefixes) {
				continue
			}
			validation.OK = false
			validation.InvalidJournalKeys++
			if len(validation.InvalidJournalKeySamples) < backupPartitionInvalidKeySampleLimit {
				validation.InvalidJournalKeySamples = append(validation.InvalidJournalKeySamples, key)
			}
		}
	}
	if validation.InvalidJournalKeys == 0 {
		return validation, nil
	}
	return validation, fmt.Errorf("hatriecache: backup partition metadata does not cover journal key %q", validation.InvalidJournalKeySamples[0])
}

func backupPartitionJournalRequestKeys(request CacheCommandRequest, keys []string) []string {
	switch strings.ToUpper(strings.TrimSpace(request.Command)) {
	case "BATCH", "INTERNALBATCH":
		for _, payload := range request.Batch {
			keys = backupPartitionJournalRequestKeys(payload, keys)
		}
		return keys
	}
	if !commandShouldJournal(request) {
		return keys
	}
	key := strings.TrimSpace(request.Key)
	if key == "" {
		return keys
	}
	return append(keys, key)
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
	out.InvalidJournalKeySamples = append([]string(nil), input.InvalidJournalKeySamples...)
	return &out
}

func verifyJournalFile(displayPath string, path string) (BackupDoctorJournal, error) {
	report, _, err := verifyJournalFileWithEntries(displayPath, path)
	return report, err
}

func verifyJournalFileWithEntries(displayPath string, path string) (BackupDoctorJournal, []commandJournalEntry, error) {
	entries, err := scanBackupJournalEntries(path)
	if err != nil {
		return BackupDoctorJournal{}, nil, err
	}
	var last uint64
	for _, entry := range entries {
		if entry.Sequence > last {
			last = entry.Sequence
		}
	}
	return BackupDoctorJournal{Path: displayPath, OK: true, Entries: len(entries), LastSequence: last}, entries, nil
}

func scanBackupJournalEntries(path string) ([]commandJournalEntry, error) {
	var entries []commandJournalEntry
	_, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func countJournalEntries(path string, segmented bool) int {
	count := 0
	if _, err := scanCommandJournalSet(path, segmented, func(commandJournalEntry) error {
		count++
		return nil
	}); err != nil {
		return 0
	}
	return count
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
