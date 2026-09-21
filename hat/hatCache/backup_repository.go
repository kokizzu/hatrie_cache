package hatCache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hatrie_cache/hat/hatBackup"
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
	BackupRepositoryVersion                = 1
	DefaultBackupRepositoryRetention       = 32
	DefaultBackupRepositoryChunkSize int64 = 1 << 20
	BackupRepositoryChunkingDisabled int64 = -1
	backupRepositoryMinChunkSize     int64 = 4 << 10
	backupRepositoryDescriptorPath         = "repository.json"
	backupRepositoryLatestPath             = "latest"
	backupRepositoryManifestsPath          = "manifests"
	backupRepositoryObjectsPath            = "objects"
	backupRepositoryFormat                 = "content-addressed-pebble-v1"
)

type backupRepositoryDescriptor struct {
	Version int    `json:"version"`
	Format  string `json:"format"`
}

var backupRepositoryLocks sync.Map

var backupRepositoryChunkBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, int(DefaultBackupRepositoryChunkSize))
	},
}

func CreateIncrementalBackupRepository(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (BackupBundleManifest, error) {
	return CreateIncrementalBackupRepositoryWithContext(context.Background(), path, trie, journal, options)
}

// CreateIncrementalBackupRepositoryWithContext creates a content-addressed
// incremental repository and stops before publication when ctx is canceled.
// Objects copied before cancellation remain reusable by the next attempt.
func CreateIncrementalBackupRepositoryWithContext(ctx context.Context, path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (BackupBundleManifest, error) {
	ctx = normalizeBackupContext(ctx)
	if err := checkBackupContext(ctx); err != nil {
		return BackupBundleManifest{}, err
	}
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
	if len(options.KeyPrefixes) > 0 {
		return BackupBundleManifest{}, errors.New("hatriecache: backup key prefixes require snapshot mode")
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
	if options.RepositoryRetainBytes < 0 {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository byte retention must be non-negative")
	}
	repositoryChunkSize, err := normalizeBackupRepositoryChunkSize(options.RepositoryChunkSize)
	if err != nil {
		return BackupBundleManifest{}, err
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
		if err := checkBackupContext(ctx); err != nil {
			return BackupBundleManifest{}, err
		}
		return createIncrementalBackupRepositoryLocked(ctx, path, trie, store, options.DirtyTracker, journal.lastSequenceLocked(), journal.format, true, partition, createdAt, retention, options.RepositoryRetainBytes, repositoryChunkSize)
	}
	return createIncrementalBackupRepositoryLocked(ctx, path, trie, store, options.DirtyTracker, 0, "", false, partition, createdAt, retention, options.RepositoryRetainBytes, repositoryChunkSize)
}

func normalizeBackupRepositoryChunkSize(value int64) (int64, error) {
	switch {
	case value == BackupRepositoryChunkingDisabled:
		return BackupRepositoryChunkingDisabled, nil
	case value == 0:
		return DefaultBackupRepositoryChunkSize, nil
	case value < backupRepositoryMinChunkSize:
		return 0, fmt.Errorf("hatriecache: backup repository chunk size must be at least %d bytes or -1 to disable chunking", backupRepositoryMinChunkSize)
	default:
		return value, nil
	}
}

func createIncrementalBackupRepositoryLocked(ctx context.Context, path string, trie *HatTrie, store *PebbleStore, tracker *LevelDBDirtyTracker, journalSequence uint64, journalFormat CommandJournalFormat, includeJournal bool, partition *BackupPartitionMetadata, createdAt time.Time, retention int, retentionBytes int64, repositoryChunkSize int64) (BackupBundleManifest, error) {
	mutexValue, _ := backupRepositoryLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := mutexValue.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()

	if err := checkBackupContext(ctx); err != nil {
		return BackupBundleManifest{}, err
	}
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
		dirty, generation, err = store.SaveIncrementalCheckpointWithJournalSequence(trie, tracker, checkpointPath, journalSequence)
	} else {
		err = store.SaveCheckpointWithJournalSequence(trie, checkpointPath, journalSequence)
		generation = store.ActiveGeneration()
	}
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if err := checkBackupContext(ctx); err != nil {
		return BackupBundleManifest{}, err
	}

	files, payloads, err := backupBundleDirectoryPayloads(backupBundleStorePath, checkpointPath)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if err := checkBackupContext(ctx); err != nil {
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
	consistency, err := hatBackup.BuildBundleConsistency(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifest.Consistency = consistency
	if err := storeBackupRepositoryObjects(ctx, path, &manifest, payloads, repositoryChunkSize); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := checkBackupContext(ctx); err != nil {
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
	if err := checkBackupContext(ctx); err != nil {
		return BackupBundleManifest{}, err
	}
	manifestPath := filepath.Join(path, backupRepositoryManifestsPath, manifest.BackupID+".json")
	if err := writeFileAtomic(manifestPath, manifestData); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := writeFileAtomic(filepath.Join(path, backupRepositoryLatestPath), []byte(manifest.BackupID+"\n")); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := pruneBackupRepositoryWithBytes(path, manifest.BackupID, retention, retentionBytes); err != nil {
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
	return marshalCommandJournalEntry(commandJournalEntry{Version: commandJournalVersion, Sequence: sequence, Checkpoint: true}, format)
}

func backupRepositoryStoreIdentity(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(filepath.Clean(absolute)))
	return hex.EncodeToString(sum[:]), nil
}

func storeBackupRepositoryObjects(ctx context.Context, root string, manifest *BackupBundleManifest, payloads []backupBundlePayloadFile, chunkSize int64) error {
	fileIndexes := make(map[string]int, len(manifest.Files))
	for index, file := range manifest.Files {
		fileIndexes[file.Path] = index
	}
	for _, payload := range payloads {
		if err := checkBackupContext(ctx); err != nil {
			return err
		}
		index, ok := fileIndexes[payload.name]
		if !ok {
			return fmt.Errorf("hatriecache: backup repository payload %s is undeclared", payload.name)
		}
		file := manifest.Files[index]
		if chunkSize > 0 && file.Size > chunkSize {
			chunks, err := storeBackupRepositoryChunkedPayload(ctx, root, file, payload, chunkSize, manifest)
			if err != nil {
				return err
			}
			manifest.Files[index].Chunks = chunks
			continue
		}
		manifest.Files[index].Chunks = nil
		if err := storeBackupRepositoryObject(ctx, root, file.SHA256, file.Size, manifest, func(writer io.Writer) error {
			return copyBackupRepositoryPayload(ctx, payload, file, writer)
		}); err != nil {
			return err
		}
	}
	return nil
}

func storeBackupRepositoryChunkedPayload(ctx context.Context, root string, file BackupBundleFile, payload backupBundlePayloadFile, chunkSize int64, manifest *BackupBundleManifest) ([]hatBackup.BundleChunk, error) {
	source, err := openBackupRepositoryPayload(payload)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	if file.Size < 0 {
		return nil, fmt.Errorf("hatriecache: backup repository payload %s has a negative size", file.Path)
	}
	buffer := makeBackupRepositoryChunkBuffer(chunkSize)
	defer releaseBackupRepositoryChunkBuffer(buffer)
	fullHash := sha256.New()
	chunkCount := file.Size / chunkSize
	if file.Size%chunkSize != 0 {
		chunkCount++
	}
	if chunkCount > int64(^uint(0)>>1) {
		return nil, errors.New("hatriecache: backup repository chunk count is too large")
	}
	chunks := make([]hatBackup.BundleChunk, 0, int(chunkCount))
	var offset int64
	for offset < file.Size {
		if err := checkBackupContext(ctx); err != nil {
			return nil, err
		}
		readSize := chunkSize
		if remaining := file.Size - offset; remaining < readSize {
			readSize = remaining
		}
		if _, err := io.ReadFull(backupContextReader{ctx: ctx, Reader: source}, buffer[:readSize]); err != nil {
			return nil, err
		}
		chunkHash := sha256.Sum256(buffer[:readSize])
		chunkSHA256 := hex.EncodeToString(chunkHash[:])
		chunk := hatBackup.BundleChunk{Offset: offset, Size: readSize, SHA256: chunkSHA256}
		if err := storeBackupRepositoryObject(ctx, root, chunkSHA256, readSize, manifest, func(writer io.Writer) error {
			_, err := backupContextWriter{ctx: ctx, Writer: writer}.Write(buffer[:readSize])
			return err
		}); err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
		_, _ = fullHash.Write(buffer[:readSize])
		offset += readSize
	}
	var extra [1]byte
	if read, readErr := (backupContextReader{ctx: ctx, Reader: source}).Read(extra[:]); read != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
		if readErr == nil {
			return nil, fmt.Errorf("hatriecache: backup repository payload %s is larger than declared", file.Path)
		}
		return nil, readErr
	}
	if offset != file.Size || hex.EncodeToString(fullHash.Sum(nil)) != file.SHA256 {
		return nil, fmt.Errorf("hatriecache: backup repository payload checksum mismatch for %s", file.Path)
	}
	return chunks, nil
}

func makeBackupRepositoryChunkBuffer(chunkSize int64) []byte {
	if chunkSize != DefaultBackupRepositoryChunkSize {
		return make([]byte, int(chunkSize))
	}
	buffer := backupRepositoryChunkBufferPool.Get().([]byte)
	return buffer[:int(chunkSize)]
}

func releaseBackupRepositoryChunkBuffer(buffer []byte) {
	if cap(buffer) == int(DefaultBackupRepositoryChunkSize) {
		backupRepositoryChunkBufferPool.Put(buffer[:int(DefaultBackupRepositoryChunkSize)])
	}
}

func storeBackupRepositoryObject(ctx context.Context, root string, hash string, size int64, manifest *BackupBundleManifest, write func(io.Writer) error) error {
	objectPath, err := backupRepositoryObjectPath(root, hash)
	if err != nil {
		return err
	}
	if info, err := os.Stat(objectPath); err == nil {
		if !info.Mode().IsRegular() || info.Size() != size {
			return fmt.Errorf("hatriecache: backup repository object size mismatch for %s", hash)
		}
		manifest.ReusedObjects++
		manifest.ReusedObjectBytes += size
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := checkBackupContext(ctx); err != nil {
		return err
	}
	if err := writeFileAtomicStream(objectPath, write); err != nil {
		return err
	}
	manifest.NewObjects++
	manifest.NewObjectBytes += size
	return nil
}

func openBackupRepositoryPayload(payload backupBundlePayloadFile) (io.ReadCloser, error) {
	if payload.path != "" {
		return os.Open(payload.path)
	}
	return io.NopCloser(bytes.NewReader(payload.data)), nil
}

func copyBackupRepositoryPayload(ctx context.Context, payload backupBundlePayloadFile, declaration BackupBundleFile, writer io.Writer) error {
	source, err := openBackupRepositoryPayload(payload)
	if err != nil {
		return err
	}
	defer source.Close()
	hash := sha256.New()
	output := backupContextWriter{ctx: ctx, Writer: io.MultiWriter(writer, hash)}
	size, err := io.Copy(output, backupContextReader{ctx: ctx, Reader: source})
	if err != nil {
		return err
	}
	if size != declaration.Size || hex.EncodeToString(hash.Sum(nil)) != declaration.SHA256 {
		return fmt.Errorf("hatriecache: backup repository payload checksum mismatch for %s", declaration.Path)
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
	for _, file := range manifest.Files {
		if err := hatBackup.ValidateBundleFileChunks(file); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	computedID, err := backupRepositoryManifestID(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if computedID != backupID {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository manifest checksum mismatch")
	}
	if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

func materializeBackupRepository(root string, backupID string, destination string) (BackupBundleManifest, error) {
	return materializeBackupRepositoryWithResume(root, backupID, destination, false)
}

func materializeBackupRepositoryWithResume(root string, backupID string, destination string, resume bool) (BackupBundleManifest, error) {
	return materializeBackupRepositoryWithConcurrency(root, backupID, destination, resume, 0)
}

func materializeBackupRepositoryWithConcurrency(root string, backupID string, destination string, resume bool, maxPartConcurrency int) (BackupBundleManifest, error) {
	manifest, err := readBackupRepositoryManifest(root, backupID)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	fileOptions := hatBackup.RestoreFileOptions{MaxConcurrency: maxPartConcurrency}
	if err := fileOptions.Validate(); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return BackupBundleManifest{}, err
	}
	if resume {
		expected := make(map[string]BackupBundleFile, len(manifest.Files))
		for _, file := range manifest.Files {
			clean, err := cleanBackupBundlePath(file.Path)
			if err != nil {
				return BackupBundleManifest{}, err
			}
			if _, exists := expected[clean]; exists {
				return BackupBundleManifest{}, fmt.Errorf("hatriecache: duplicate backup file declaration %s", clean)
			}
			expected[clean] = file
		}
		if err := pruneRestoreStaging(destination, expected); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	if resume {
		for _, file := range manifest.Files {
			clean, err := cleanBackupBundlePath(file.Path)
			if err != nil {
				return BackupBundleManifest{}, err
			}
			if len(file.Chunks) > 0 {
				target := filepath.Join(destination, filepath.FromSlash(clean))
				if err := restoreBackupRepositoryChunkedFile(root, target, file, true); err != nil {
					return BackupBundleManifest{}, err
				}
				continue
			}
			objectPath, err := backupRepositoryObjectPath(root, file.SHA256)
			if err != nil {
				return BackupBundleManifest{}, err
			}
			target := filepath.Join(destination, filepath.FromSlash(clean))
			if err := rejectRestoreSymlinkComponents(filepath.Dir(target)); err != nil {
				return BackupBundleManifest{}, err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return BackupBundleManifest{}, err
			}
			if err := copyBackupRepositoryObjectWithResume(objectPath, target, file, true); err != nil {
				return BackupBundleManifest{}, err
			}
		}
		return manifest, nil
	}

	files := make([]hatBackup.RestoreFile, 0, len(manifest.Files))
	for _, file := range manifest.Files {
		clean, err := cleanBackupBundlePath(file.Path)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		if len(file.Chunks) > 0 {
			target := filepath.Join(destination, filepath.FromSlash(clean))
			if err := restoreBackupRepositoryChunkedFile(root, target, file, false); err != nil {
				return BackupBundleManifest{}, err
			}
			continue
		}
		objectPath, err := backupRepositoryObjectPath(root, file.SHA256)
		if err != nil {
			return BackupBundleManifest{}, err
		}
		target := filepath.Join(destination, filepath.FromSlash(clean))
		files = append(files, hatBackup.RestoreFile{Source: objectPath, Destination: target, Size: file.Size, SHA256: file.SHA256})
	}
	if len(files) > 0 {
		if err := hatBackup.CopyRestoreFiles(files, fileOptions); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	return manifest, nil
}

func restoreBackupRepositoryChunkedFile(root string, targetPath string, declaration BackupBundleFile, resume bool) error {
	if err := hatBackup.ValidateBundleFileChunks(declaration); err != nil {
		return err
	}
	if err := rejectRestoreSymlinkComponents(filepath.Dir(targetPath)); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(targetPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore target is not a regular file: %s", targetPath)
		}
		if !resume {
			return os.ErrExist
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	flags := os.O_CREATE | os.O_RDWR
	if !resume {
		flags |= os.O_EXCL
	}
	target, err := os.OpenFile(targetPath, flags, 0o600)
	if err != nil {
		return err
	}
	removeOnError := !resume
	defer func() {
		_ = target.Close()
		if removeOnError {
			_ = os.Remove(targetPath)
		}
	}()
	if err := target.Truncate(0); err != nil {
		return err
	}
	fullHash := sha256.New()
	for _, chunk := range declaration.Chunks {
		objectPath, err := backupRepositoryObjectPath(root, chunk.SHA256)
		if err != nil {
			return err
		}
		info, err := os.Stat(objectPath)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != chunk.Size {
			return fmt.Errorf("hatriecache: backup repository chunk size mismatch for %s", declaration.Path)
		}
		source, err := os.Open(objectPath)
		if err != nil {
			return err
		}
		chunkHash := sha256.New()
		writer := io.NewOffsetWriter(target, chunk.Offset)
		written, copyErr := io.CopyN(io.MultiWriter(writer, fullHash, chunkHash), source, chunk.Size)
		closeErr := source.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written != chunk.Size || hex.EncodeToString(chunkHash.Sum(nil)) != chunk.SHA256 {
			return fmt.Errorf("hatriecache: backup repository chunk checksum mismatch for %s", declaration.Path)
		}
	}
	if err := target.Truncate(declaration.Size); err != nil {
		return err
	}
	if err := target.Sync(); err != nil {
		return err
	}
	if hex.EncodeToString(fullHash.Sum(nil)) != declaration.SHA256 {
		return fmt.Errorf("hatriecache: backup repository file checksum mismatch for %s", declaration.Path)
	}
	if err := target.Close(); err != nil {
		return err
	}
	removeOnError = false
	return nil
}

func copyBackupRepositoryObjectWithResume(sourcePath string, targetPath string, declaration BackupBundleFile, resume bool) error {
	if !resume {
		return copyBackupRepositoryObject(sourcePath, targetPath, declaration)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	return extractBackupBundlePayload(source, targetPath, declaration, true)
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
	return pruneBackupRepositoryWithBytes(root, latest, retention, 0)
}

func pruneBackupRepositoryWithBytes(root string, latest string, retention int, retentionBytes int64) error {
	keep := make(map[string]BackupBundleManifest, retention)
	ordered := make([]string, 0, retention)
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
		ordered = append(ordered, current)
		current = manifest.ParentBackupID
	}
	for retentionBytes > 0 && len(ordered) > 1 && backupRepositoryRetainedBytes(keep) > retentionBytes {
		oldest := ordered[len(ordered)-1]
		delete(keep, oldest)
		ordered = ordered[:len(ordered)-1]
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
			if len(file.Chunks) == 0 {
				reachable[file.SHA256] = struct{}{}
				continue
			}
			for _, chunk := range file.Chunks {
				reachable[chunk.SHA256] = struct{}{}
			}
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

func backupRepositoryRetainedBytes(manifests map[string]BackupBundleManifest) int64 {
	reachable := make(map[string]struct{})
	var total int64
	for _, manifest := range manifests {
		for _, file := range manifest.Files {
			objects := []struct {
				hash string
				size int64
			}{{hash: file.SHA256, size: file.Size}}
			if len(file.Chunks) > 0 {
				objects = objects[:0]
				for _, chunk := range file.Chunks {
					objects = append(objects, struct {
						hash string
						size int64
					}{hash: chunk.SHA256, size: chunk.Size})
				}
			}
			for _, object := range objects {
				if _, seen := reachable[object.hash]; seen {
					continue
				}
				reachable[object.hash] = struct{}{}
				if object.size > 0 && total <= int64(^uint64(0)>>1)-object.size {
					total += object.size
				} else if object.size > 0 {
					return int64(^uint64(0) >> 1)
				}
			}
		}
	}
	return total
}
