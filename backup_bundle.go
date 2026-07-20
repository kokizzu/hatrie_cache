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
	"sort"
	"strings"
	"time"

	"hatrie_cache/internal/jsonwire"
)

const (
	BackupBundleVersion      = 1
	backupBundleManifestPath = "manifest.json"
	backupBundleSnapshotPath = "snapshot.hc"
	backupBundleJournalPath  = "commands.journal"
	backupBundleStorePath    = "cache.leveldb"
)

type BackupMode string

const (
	BackupModeAuto              BackupMode = "auto"
	BackupModeSnapshot          BackupMode = "snapshot"
	BackupModePebbleCheckpoint  BackupMode = "pebble-checkpoint"
	BackupModePebbleIncremental BackupMode = "pebble-incremental"
)

type BackupBundleOptions struct {
	SnapshotFormat   SnapshotFormat
	CreatedAt        time.Time
	Partition        BackupPartitionMetadata
	Mode             BackupMode
	PersistentStore  PersistentStore
	DirtyTracker     *LevelDBDirtyTracker
	RepositoryRetain int
}

type BackupPartitionMetadata struct {
	Mode                string   `json:"mode,omitempty"`
	Partitions          []string `json:"partitions,omitempty"`
	NodeID              string   `json:"node_id,omitempty"`
	TopologyEpoch       uint64   `json:"topology_epoch,omitempty"`
	TopologyFingerprint string   `json:"topology_fingerprint,omitempty"`
	KeyPrefixes         []string `json:"key_prefixes,omitempty"`
}

type BackupBundleManifest struct {
	Version           int                      `json:"version"`
	CreatedAt         time.Time                `json:"created_at"`
	Mode              BackupMode               `json:"mode,omitempty"`
	Snapshot          string                   `json:"snapshot,omitempty"`
	SnapshotFormat    string                   `json:"snapshot_format,omitempty"`
	Store             string                   `json:"store,omitempty"`
	StorageBackend    string                   `json:"storage_backend,omitempty"`
	StorageFormat     string                   `json:"storage_format,omitempty"`
	StorageGeneration uint64                   `json:"storage_generation,omitempty"`
	StorageIdentity   string                   `json:"storage_identity,omitempty"`
	BackupID          string                   `json:"backup_id,omitempty"`
	ParentBackupID    string                   `json:"parent_backup_id,omitempty"`
	Incremental       bool                     `json:"incremental,omitempty"`
	NewObjects        int                      `json:"new_objects,omitempty"`
	ReusedObjects     int                      `json:"reused_objects,omitempty"`
	NewObjectBytes    int64                    `json:"new_object_bytes,omitempty"`
	ReusedObjectBytes int64                    `json:"reused_object_bytes,omitempty"`
	Journal           string                   `json:"journal,omitempty"`
	JournalFormat     string                   `json:"journal_format,omitempty"`
	JournalSequence   uint64                   `json:"journal_sequence"`
	Partition         *BackupPartitionMetadata `json:"partition,omitempty"`
	Files             []BackupBundleFile       `json:"files"`
	RestoreHint       string                   `json:"restore_hint"`
}

func ParseBackupMode(value string) (BackupMode, error) {
	switch BackupMode(strings.ToLower(strings.TrimSpace(value))) {
	case "", BackupModeAuto:
		return BackupModeAuto, nil
	case BackupModeSnapshot:
		return BackupModeSnapshot, nil
	case BackupModePebbleCheckpoint:
		return BackupModePebbleCheckpoint, nil
	case BackupModePebbleIncremental:
		return BackupModePebbleIncremental, nil
	default:
		return "", errors.New("hatriecache: backup mode must be auto, snapshot, pebble-checkpoint, or pebble-incremental")
	}
}

type BackupBundleFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func normalizeBackupPartitionMetadata(input BackupPartitionMetadata) (*BackupPartitionMetadata, error) {
	out := BackupPartitionMetadata{
		Mode:                strings.TrimSpace(input.Mode),
		NodeID:              strings.TrimSpace(input.NodeID),
		TopologyEpoch:       input.TopologyEpoch,
		TopologyFingerprint: strings.TrimSpace(input.TopologyFingerprint),
	}
	var err error
	out.Partitions, err = normalizeBackupPartitionList("partition", input.Partitions)
	if err != nil {
		return nil, err
	}
	out.KeyPrefixes, err = normalizeBackupPartitionList("key prefix", input.KeyPrefixes)
	if err != nil {
		return nil, err
	}
	if out.Mode == "" && out.NodeID == "" && out.TopologyEpoch == 0 && out.TopologyFingerprint == "" && len(out.Partitions) == 0 && len(out.KeyPrefixes) == 0 {
		return nil, nil
	}
	if out.Mode == "" {
		out.Mode = "partitioned"
	}
	if len(out.Partitions) == 0 {
		return nil, errors.New("hatriecache: backup partition metadata requires at least one partition id")
	}
	return &out, nil
}

func normalizeBackupPartitionList(label string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("hatriecache: backup %s id is required", label)
		}
		if _, ok := seen[value]; ok {
			return nil, fmt.Errorf("hatriecache: duplicate backup %s %q", label, value)
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func cloneBackupPartitionMetadata(input *BackupPartitionMetadata) *BackupPartitionMetadata {
	if input == nil {
		return nil
	}
	out := *input
	out.Partitions = append([]string(nil), input.Partitions...)
	out.KeyPrefixes = append([]string(nil), input.KeyPrefixes...)
	return &out
}

func CreateBackupBundle(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (BackupBundleManifest, error) {
	if path == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: backup bundle path is required")
	}
	if trie == nil {
		return BackupBundleManifest{}, ErrNilHatTrie
	}
	mode, err := ParseBackupMode(string(options.Mode))
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if mode == BackupModePebbleIncremental {
		options.Mode = mode
		return CreateIncrementalBackupRepository(path, trie, journal, options)
	}
	if mode == BackupModeAuto {
		mode = BackupModeSnapshot
	}
	var snapshotFormat SnapshotFormat
	if mode == BackupModeSnapshot {
		snapshotFormat, err = ParseSnapshotFormat(string(options.SnapshotFormat))
		if err != nil {
			return BackupBundleManifest{}, err
		}
	} else if _, ok := options.PersistentStore.(*PebbleStore); !ok {
		return BackupBundleManifest{}, errors.New("hatriecache: pebble-checkpoint backup mode requires a Pebble persistent store")
	}
	partition, err := normalizeBackupPartitionMetadata(options.Partition)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	createdAt := options.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	createdAt = createdAt.UTC()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return BackupBundleManifest{}, err
	}
	tmpDir, err := os.MkdirTemp(dir, filepath.Base(path)+".work-*")
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer os.RemoveAll(tmpDir)

	if journal != nil {
		journal.mu.Lock()
		defer journal.mu.Unlock()
		if journal.closed {
			return BackupBundleManifest{}, ErrCommandJournalClosed
		}
		return createBackupBundleLocked(path, tmpDir, trie, journal.lastSequenceLocked(), journal.format, snapshotFormat, createdAt, true, partition, mode, options.PersistentStore)
	}
	return createBackupBundleLocked(path, tmpDir, trie, 0, "", snapshotFormat, createdAt, false, partition, mode, options.PersistentStore)
}

type backupBundlePayloadFile struct {
	name string
	path string
	data []byte
}

func createBackupBundleLocked(path string, tmpDir string, trie *HatTrie, journalSequence uint64, journalFormat CommandJournalFormat, snapshotFormat SnapshotFormat, createdAt time.Time, includeJournal bool, partition *BackupPartitionMetadata, mode BackupMode, persistentStore PersistentStore) (BackupBundleManifest, error) {
	files := make([]BackupBundleFile, 0)
	payloads := make([]backupBundlePayloadFile, 0)
	manifest := BackupBundleManifest{
		Version:         BackupBundleVersion,
		CreatedAt:       createdAt,
		Mode:            mode,
		JournalSequence: journalSequence,
		Partition:       cloneBackupPartitionMetadata(partition),
	}
	switch mode {
	case BackupModeSnapshot:
		snapshotPath := filepath.Join(tmpDir, backupBundleSnapshotPath)
		if err := trie.SaveSnapshotWithJournalSequenceAndFormat(snapshotPath, journalSequence, snapshotFormat); err != nil {
			return BackupBundleManifest{}, err
		}
		snapshotFile, err := backupBundleFileInfo(backupBundleSnapshotPath, snapshotPath)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		files = append(files, snapshotFile)
		payloads = append(payloads, backupBundlePayloadFile{name: backupBundleSnapshotPath, path: snapshotPath})
		manifest.Snapshot = backupBundleSnapshotPath
		manifest.SnapshotFormat = string(snapshotFormat)
		manifest.RestoreHint = "extract snapshot.hc and commands.journal into DATA_DIR, then start with SNAPSHOT_PATH=DATA_DIR/snapshot.hc JOURNAL_PATH=DATA_DIR/commands.journal"
	case BackupModePebbleCheckpoint:
		store := persistentStore.(*PebbleStore)
		checkpointPath := filepath.Join(tmpDir, backupBundleStorePath)
		if err := store.SaveCheckpoint(trie, checkpointPath); err != nil {
			return BackupBundleManifest{}, err
		}
		checkpointFiles, checkpointPayloads, err := backupBundleDirectoryPayloads(backupBundleStorePath, checkpointPath)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		files = append(files, checkpointFiles...)
		payloads = append(payloads, checkpointPayloads...)
		markerName := backupBundleStorePath + storageBackendMarkerSuffix
		markerData := []byte(string(StorageBackendPebble) + "\n")
		files = append(files, backupBundleBytesInfo(markerName, markerData))
		payloads = append(payloads, backupBundlePayloadFile{name: markerName, data: markerData})
		manifest.Store = backupBundleStorePath
		manifest.StorageBackend = string(StorageBackendPebble)
		manifest.StorageFormat = string(store.Format())
		manifest.RestoreHint = "extract cache.leveldb and cache.leveldb.backend into DATA_DIR, then start with DB_PATH=DATA_DIR/cache.leveldb DB_BACKEND=auto"
	default:
		return BackupBundleManifest{}, fmt.Errorf("hatriecache: unsupported backup mode %q", mode)
	}

	var journalData []byte
	if includeJournal {
		var journalBuffer bytes.Buffer
		if journalSequence > 0 {
			if err := writeCommandJournalEntry(&journalBuffer, commandJournalEntry{
				Version:    commandJournalVersion,
				Sequence:   journalSequence,
				Checkpoint: true,
			}, journalFormat); err != nil {
				return BackupBundleManifest{}, err
			}
		}
		journalData = journalBuffer.Bytes()
		files = append(files, backupBundleBytesInfo(backupBundleJournalPath, journalData))
		payloads = append(payloads, backupBundlePayloadFile{name: backupBundleJournalPath, data: journalData})
	}

	manifest.Files = files
	if includeJournal {
		manifest.Journal = backupBundleJournalPath
		manifest.JournalFormat = string(journalFormat)
	}

	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifestData = append(manifestData, '\n')

	if err := writeFileAtomicStream(path, func(writer io.Writer) error {
		return writeBackupBundlePayloadTarGzip(writer, payloads, manifestData, createdAt)
	}); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

func backupBundleDirectoryPayloads(prefix string, root string) ([]BackupBundleFile, []backupBundlePayloadFile, error) {
	paths := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: backup checkpoint contains non-regular file %s", path)
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(paths)
	files := make([]BackupBundleFile, 0, len(paths))
	payloads := make([]backupBundlePayloadFile, 0, len(paths))
	for _, path := range paths {
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil, nil, err
		}
		name := filepath.ToSlash(filepath.Join(prefix, relative))
		file, err := backupBundleFileInfo(name, path)
		if err != nil {
			return nil, nil, err
		}
		files = append(files, file)
		payloads = append(payloads, backupBundlePayloadFile{name: name, path: path})
	}
	return files, payloads, nil
}

func writeBackupBundlePayloadTarGzip(writer io.Writer, payloads []backupBundlePayloadFile, manifestData []byte, modTime time.Time) error {
	gzipWriter, err := gzip.NewWriterLevel(writer, gzip.BestSpeed)
	if err != nil {
		return err
	}
	tarWriter := tar.NewWriter(gzipWriter)
	closeWithError := func(err error) error {
		_ = tarWriter.Close()
		_ = gzipWriter.Close()
		return err
	}
	if err := writeBackupBundleBytes(tarWriter, backupBundleManifestPath, manifestData, modTime); err != nil {
		return closeWithError(err)
	}
	for _, payload := range payloads {
		var err error
		if payload.path != "" {
			err = writeBackupBundleFile(tarWriter, payload.name, payload.path, modTime)
		} else {
			err = writeBackupBundleBytes(tarWriter, payload.name, payload.data, modTime)
		}
		if err != nil {
			return closeWithError(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		_ = gzipWriter.Close()
		return err
	}
	return gzipWriter.Close()
}

func writeBackupBundleTarGzip(writer io.Writer, snapshotPath string, journalData []byte, manifestData []byte, modTime time.Time, includeJournal bool) error {
	payloads := []backupBundlePayloadFile{{name: backupBundleSnapshotPath, path: snapshotPath}}
	if includeJournal {
		payloads = append(payloads, backupBundlePayloadFile{name: backupBundleJournalPath, data: journalData})
	}
	return writeBackupBundlePayloadTarGzip(writer, payloads, manifestData, modTime)
}

func writeBackupBundleFile(writer *tar.Writer, name string, path string, modTime time.Time) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("hatriecache: backup bundle file %s is not regular", path)
	}
	header := &tar.Header{
		Name:    name,
		Mode:    0o600,
		Size:    info.Size(),
		ModTime: modTime,
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}

func writeBackupBundleBytes(writer *tar.Writer, name string, data []byte, modTime time.Time) error {
	header := &tar.Header{
		Name:    name,
		Mode:    0o600,
		Size:    int64(len(data)),
		ModTime: modTime,
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func backupBundleFileInfo(name string, path string) (BackupBundleFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return BackupBundleFile{}, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return BackupBundleFile{}, err
	}
	return BackupBundleFile{Path: name, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func backupBundleBytesInfo(name string, data []byte) BackupBundleFile {
	sum := sha256.Sum256(data)
	return BackupBundleFile{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}
