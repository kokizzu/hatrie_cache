package hatriecache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"

	"hatrie_cache/internal/jsonwire"
)

const (
	BackupRepositoryVersion          = 1
	DefaultBackupRepositoryRetention = 32
	backupRepositoryDescriptorPath   = "repository.json"
	backupRepositoryLatestPath       = "latest"
	backupRepositoryManifestsPath    = "manifests"
	backupRepositoryObjectsPath      = "objects"
	backupRepositoryFormat           = "content-addressed-pebble-v1"
)

type backupRepositoryDescriptor struct {
	Version int    `json:"version"`
	Format  string `json:"format"`
}

var backupRepositoryLocks sync.Map

func CreateIncrementalBackupRepository(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (BackupBundleManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository path is required")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	path = filepath.Clean(absolutePath)
	if trie == nil {
		return BackupBundleManifest{}, ErrNilHatTrie
	}
	store, ok := options.PersistentStore.(*PebbleStore)
	if !ok {
		return BackupBundleManifest{}, errors.New("hatriecache: pebble-incremental backup mode requires a Pebble persistent store")
	}
	if options.DirtyTracker == nil {
		return BackupBundleManifest{}, errors.New("hatriecache: pebble-incremental backup mode requires a dirty tracker")
	}
	retention := options.RepositoryRetain
	if retention == 0 {
		retention = DefaultBackupRepositoryRetention
	}
	if retention < 1 {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository retention must be positive")
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

	if journal != nil {
		journal.mu.Lock()
		defer journal.mu.Unlock()
		if journal.closed {
			return BackupBundleManifest{}, ErrCommandJournalClosed
		}
		return createIncrementalBackupRepositoryLocked(path, trie, store, options.DirtyTracker, journal.lastSequenceLocked(), journal.format, true, partition, createdAt, retention)
	}
	return createIncrementalBackupRepositoryLocked(path, trie, store, options.DirtyTracker, 0, "", false, partition, createdAt, retention)
}

func createIncrementalBackupRepositoryLocked(path string, trie *HatTrie, store *PebbleStore, tracker *LevelDBDirtyTracker, journalSequence uint64, journalFormat CommandJournalFormat, includeJournal bool, partition *BackupPartitionMetadata, createdAt time.Time, retention int) (BackupBundleManifest, error) {
	mutexValue, _ := backupRepositoryLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := mutexValue.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()

	if err := ensureBackupRepository(path); err != nil {
		return BackupBundleManifest{}, err
	}
	previous, previousErr := readBackupRepositoryManifest(path, "")
	if previousErr != nil && !errors.Is(previousErr, os.ErrNotExist) {
		return BackupBundleManifest{}, previousErr
	}
	storeIdentity, err := backupRepositoryStoreIdentity(store.Path())
	if err != nil {
		return BackupBundleManifest{}, err
	}
	canIncrement := previousErr == nil &&
		previous.Mode == BackupModePebbleIncremental &&
		previous.StorageBackend == string(StorageBackendPebble) &&
		previous.StorageGeneration != 0 &&
		previous.StorageGeneration == store.ActiveGeneration() &&
		previous.StorageIdentity == storeIdentity

	workDir, err := os.MkdirTemp(path, ".backup-work-*")
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer os.RemoveAll(workDir)
	checkpointPath := filepath.Join(workDir, backupBundleStorePath)
	dirty := tracker.Snapshot()
	generation := uint64(0)
	if canIncrement {
		dirty, generation, err = store.SaveIncrementalCheckpoint(trie, tracker, checkpointPath)
	} else {
		err = store.SaveCheckpoint(trie, checkpointPath)
		generation = store.ActiveGeneration()
	}
	if err != nil {
		return BackupBundleManifest{}, err
	}

	files, payloads, err := backupBundleDirectoryPayloads(backupBundleStorePath, checkpointPath)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	markerName := backupBundleStorePath + storageBackendMarkerSuffix
	markerData := []byte(string(StorageBackendPebble) + "\n")
	files = append(files, backupBundleBytesInfo(markerName, markerData))
	payloads = append(payloads, backupBundlePayloadFile{name: markerName, data: markerData})

	manifest := BackupBundleManifest{
		Version:           BackupBundleVersion,
		CreatedAt:         createdAt,
		Mode:              BackupModePebbleIncremental,
		Store:             backupBundleStorePath,
		StorageBackend:    string(StorageBackendPebble),
		StorageFormat:     string(store.Format()),
		StorageGeneration: generation,
		StorageIdentity:   storeIdentity,
		Incremental:       canIncrement,
		JournalSequence:   journalSequence,
		Partition:         cloneBackupPartitionMetadata(partition),
		Files:             files,
		RestoreHint:       "restore the repository with restore-bundle, then start with DB_PATH=DATA_DIR/cache.leveldb DB_BACKEND=auto",
	}
	if previousErr == nil {
		manifest.ParentBackupID = previous.BackupID
	}
	if includeJournal {
		journalData, err := backupRepositoryJournalCheckpoint(journalSequence, journalFormat)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		manifest.Journal = backupBundleJournalPath
		manifest.JournalFormat = string(journalFormat)
		manifest.Files = append(manifest.Files, backupBundleBytesInfo(backupBundleJournalPath, journalData))
		payloads = append(payloads, backupBundlePayloadFile{name: backupBundleJournalPath, data: journalData})
	}
	if err := storeBackupRepositoryObjects(path, &manifest, payloads); err != nil {
		return BackupBundleManifest{}, err
	}
	manifest.BackupID, err = backupRepositoryManifestID(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifestData = append(manifestData, '\n')
	manifestPath := filepath.Join(path, backupRepositoryManifestsPath, manifest.BackupID+".json")
	if err := writeFileAtomic(manifestPath, manifestData); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := writeFileAtomic(filepath.Join(path, backupRepositoryLatestPath), []byte(manifest.BackupID+"\n")); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := pruneBackupRepository(path, manifest.BackupID, retention); err != nil {
		return BackupBundleManifest{}, err
	}
	tracker.Clear(dirty)
	return manifest, nil
}

func ensureBackupRepository(path string) error {
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return fmt.Errorf("hatriecache: backup repository path is not a directory: %s", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, backupRepositoryManifestsPath), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, backupRepositoryObjectsPath), 0o700); err != nil {
		return err
	}
	descriptorPath := filepath.Join(path, backupRepositoryDescriptorPath)
	if err := verifyBackupRepositoryDescriptor(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := jsonwire.Marshal(backupRepositoryDescriptor{Version: BackupRepositoryVersion, Format: backupRepositoryFormat})
	if err != nil {
		return err
	}
	return writeFileAtomic(descriptorPath, append(data, '\n'))
}

func verifyBackupRepositoryDescriptor(path string) error {
	data, err := os.ReadFile(filepath.Join(path, backupRepositoryDescriptorPath))
	if err != nil {
		return err
	}
	var descriptor backupRepositoryDescriptor
	if err := json.Unmarshal(data, &descriptor); err != nil {
		return err
	}
	if descriptor.Version != BackupRepositoryVersion || descriptor.Format != backupRepositoryFormat {
		return errors.New("hatriecache: unsupported backup repository format")
	}
	return nil
}

func backupRepositoryJournalCheckpoint(sequence uint64, format CommandJournalFormat) ([]byte, error) {
	if sequence == 0 {
		return nil, nil
	}
	var buffer strings.Builder
	if err := writeCommandJournalEntry(&buffer, commandJournalEntry{Version: commandJournalVersion, Sequence: sequence, Checkpoint: true}, format); err != nil {
		return nil, err
	}
	return []byte(buffer.String()), nil
}

func backupRepositoryStoreIdentity(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(filepath.Clean(absolute)))
	return hex.EncodeToString(sum[:]), nil
}

func storeBackupRepositoryObjects(root string, manifest *BackupBundleManifest, payloads []backupBundlePayloadFile) error {
	files := make(map[string]BackupBundleFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	for _, payload := range payloads {
		file, ok := files[payload.name]
		if !ok {
			return fmt.Errorf("hatriecache: backup repository payload %s is undeclared", payload.name)
		}
		objectPath, err := backupRepositoryObjectPath(root, file.SHA256)
		if err != nil {
			return err
		}
		if info, err := os.Stat(objectPath); err == nil {
			if !info.Mode().IsRegular() || info.Size() != file.Size {
				return fmt.Errorf("hatriecache: backup repository object size mismatch for %s", file.SHA256)
			}
			manifest.ReusedObjects++
			manifest.ReusedObjectBytes += file.Size
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := writeFileAtomicStream(objectPath, func(writer io.Writer) error {
			if payload.path != "" {
				source, err := os.Open(payload.path)
				if err != nil {
					return err
				}
				defer source.Close()
				_, err = io.Copy(writer, source)
				return err
			}
			_, err := writer.Write(payload.data)
			return err
		}); err != nil {
			return err
		}
		manifest.NewObjects++
		manifest.NewObjectBytes += file.Size
	}
	return nil
}

func backupRepositoryManifestID(manifest BackupBundleManifest) (string, error) {
	manifest.BackupID = ""
	data, err := jsonwire.Marshal(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func backupRepositoryObjectPath(root string, hash string) (string, error) {
	if len(hash) != sha256.Size*2 {
		return "", errors.New("hatriecache: invalid backup repository object hash")
	}
	if _, err := hex.DecodeString(hash); err != nil || strings.ToLower(hash) != hash {
		return "", errors.New("hatriecache: invalid backup repository object hash")
	}
	return filepath.Join(root, backupRepositoryObjectsPath, hash[:2], hash[2:]), nil
}

func readBackupRepositoryManifest(root string, backupID string) (BackupBundleManifest, error) {
	if strings.TrimSpace(backupID) == "" {
		data, err := os.ReadFile(filepath.Join(root, backupRepositoryLatestPath))
		if err != nil {
			return BackupBundleManifest{}, err
		}
		backupID = strings.TrimSpace(string(data))
	}
	if _, err := backupRepositoryObjectPath(root, backupID); err != nil {
		return BackupBundleManifest{}, errors.New("hatriecache: invalid backup repository manifest id")
	}
	data, err := os.ReadFile(filepath.Join(root, backupRepositoryManifestsPath, backupID+".json"))
	if err != nil {
		return BackupBundleManifest{}, err
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BackupBundleManifest{}, err
	}
	if manifest.Version != BackupBundleVersion || manifest.Mode != BackupModePebbleIncremental || manifest.BackupID != backupID {
		return BackupBundleManifest{}, errors.New("hatriecache: invalid backup repository manifest")
	}
	computedID, err := backupRepositoryManifestID(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if computedID != backupID {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository manifest checksum mismatch")
	}
	return manifest, nil
}

func materializeBackupRepository(root string, backupID string, destination string) (BackupBundleManifest, error) {
	manifest, err := readBackupRepositoryManifest(root, backupID)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return BackupBundleManifest{}, err
	}
	for _, file := range manifest.Files {
		clean, err := cleanBackupBundlePath(file.Path)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		objectPath, err := backupRepositoryObjectPath(root, file.SHA256)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		target := filepath.Join(destination, filepath.FromSlash(clean))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return BackupBundleManifest{}, err
		}
		if err := copyBackupRepositoryObject(objectPath, target, file); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	return manifest, nil
}

func copyBackupRepositoryObject(sourcePath string, targetPath string, declaration BackupBundleFile) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(target, hash), source)
	closeErr := target.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if size != declaration.Size || hex.EncodeToString(hash.Sum(nil)) != declaration.SHA256 {
		return fmt.Errorf("hatriecache: backup repository object checksum mismatch for %s", declaration.Path)
	}
	return nil
}

func pruneBackupRepository(root string, latest string, retention int) error {
	keep := make(map[string]BackupBundleManifest, retention)
	current := latest
	for len(keep) < retention && current != "" {
		manifest, err := readBackupRepositoryManifest(root, current)
		if err != nil {
			if len(keep) == 0 {
				return err
			}
			break
		}
		keep[current] = manifest
		current = manifest.ParentBackupID
	}
	manifestDir := filepath.Join(root, backupRepositoryManifestsPath)
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if _, ok := keep[id]; !ok {
			if err := os.Remove(filepath.Join(manifestDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	reachable := make(map[string]struct{})
	for _, manifest := range keep {
		for _, file := range manifest.Files {
			reachable[file.SHA256] = struct{}{}
		}
	}
	objectsRoot := filepath.Join(root, backupRepositoryObjectsPath)
	var objectPaths []string
	err = filepath.WalkDir(objectsRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(objectsRoot, path)
		if err != nil {
			return err
		}
		hash := strings.ReplaceAll(filepath.ToSlash(relative), "/", "")
		if _, ok := reachable[hash]; !ok {
			objectPaths = append(objectPaths, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(objectPaths)
	for _, path := range objectPaths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(root)
}
