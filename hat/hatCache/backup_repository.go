package hatCache

import (
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
	BackupRepositoryVersion          = 1
	DefaultBackupRepositoryRetention = 32
	// DefaultBackupRepositoryChunkSize enables low-overhead fixed-size
	// deduplication for large repository payloads.
	DefaultBackupRepositoryChunkSize = 1 << 20
	// DisableBackupRepositoryChunking selects the legacy whole-file object
	// layout for incremental repositories.
	DisableBackupRepositoryChunking = -1
	backupRepositoryMinChunkSize    = 4 << 10
	backupRepositoryMaxChunkSize    = 64 << 20
	backupRepositoryDescriptorPath  = "repository.json"
	backupRepositoryLatestPath      = "latest"
	backupRepositoryManifestsPath   = "manifests"
	backupRepositoryObjectsPath     = "objects"
	backupRepositoryFormat          = "content-addressed-pebble-v1"
)

func normalizeBackupRepositoryChunkSize(size int) (int, error) {
	if size == DisableBackupRepositoryChunking {
		return 0, nil
	}
	if size == 0 {
		return DefaultBackupRepositoryChunkSize, nil
	}
	if size < backupRepositoryMinChunkSize || size > backupRepositoryMaxChunkSize {
		return 0, fmt.Errorf("hatriecache: backup repository chunk size must be between %d and %d bytes", backupRepositoryMinChunkSize, backupRepositoryMaxChunkSize)
	}
	return size, nil
}

type backupRepositoryDescriptor struct {
	Version int    `json:"version"`
	Format  string `json:"format"`
}

var backupRepositoryLocks sync.Map

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
	chunkSize, err := normalizeBackupRepositoryChunkSize(options.RepositoryChunkSize)
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
		return createIncrementalBackupRepositoryLocked(ctx, path, trie, store, options.DirtyTracker, journal.lastSequenceLocked(), journal.format, true, partition, createdAt, retention, options.RepositoryRetainBytes, chunkSize)
	}
	return createIncrementalBackupRepositoryLocked(ctx, path, trie, store, options.DirtyTracker, 0, "", false, partition, createdAt, retention, options.RepositoryRetainBytes, chunkSize)
}

func createIncrementalBackupRepositoryLocked(ctx context.Context, path string, trie *HatTrie, store *PebbleStore, tracker *LevelDBDirtyTracker, journalSequence uint64, journalFormat CommandJournalFormat, includeJournal bool, partition *BackupPartitionMetadata, createdAt time.Time, retention int, retentionBytes int64, chunkSize int) (BackupBundleManifest, error) {
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
		Version:             BackupBundleVersion,
		CreatedAt:           createdAt,
		Mode:                BackupModePebbleIncremental,
		Store:               backupBundleStorePath,
		StorageBackend:      string(StorageBackendPebble),
		StorageFormat:       string(store.Format()),
		StorageGeneration:   generation,
		StorageIdentity:     storeIdentity,
		Incremental:         canIncrement,
		RepositoryChunkSize: int64(chunkSize),
		JournalSequence:     journalSequence,
		Partition:           cloneBackupPartitionMetadata(partition),
		Files:               files,
		RestoreHint:         "restore the repository with restore-bundle, then start with DB_PATH=DATA_DIR/cache.leveldb DB_BACKEND=auto",
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
	if chunkSize > 0 {
		if err := applyBackupRepositoryChunking(ctx, &manifest, payloads, chunkSize); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	consistency, err := hatBackup.BuildBundleConsistency(manifest)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	manifest.Consistency = consistency
	if err := storeBackupRepositoryObjects(ctx, path, &manifest, payloads); err != nil {
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

func applyBackupRepositoryChunking(ctx context.Context, manifest *BackupBundleManifest, payloads []backupBundlePayloadFile, chunkSize int) error {
	if manifest == nil {
		return errors.New("hatriecache: backup repository manifest is required")
	}
	if chunkSize < backupRepositoryMinChunkSize || chunkSize > backupRepositoryMaxChunkSize {
		return errors.New("hatriecache: invalid backup repository chunk size")
	}
	payloadByName := make(map[string]backupBundlePayloadFile, len(payloads))
	for _, payload := range payloads {
		payloadByName[payload.name] = payload
	}
	manifest.RepositoryChunkSize = int64(chunkSize)
	for index := range manifest.Files {
		file := &manifest.Files[index]
		if file.Size <= int64(chunkSize) {
			continue
		}
		payload, ok := payloadByName[file.Path]
		if !ok {
			return fmt.Errorf("hatriecache: backup repository payload %s is undeclared", file.Path)
		}
		chunks, err := backupRepositoryPayloadChunks(ctx, payload, file.Size, file.SHA256, chunkSize)
		if err != nil {
			return err
		}
		file.Chunks = chunks
	}
	return nil
}

func backupRepositoryPayloadChunks(ctx context.Context, payload backupBundlePayloadFile, expectedSize int64, expectedSHA256 string, chunkSize int) ([]BackupBundleChunk, error) {
	if expectedSize <= int64(chunkSize) {
		return nil, errors.New("hatriecache: backup repository payload is too small for chunking")
	}
	var source *os.File
	if payload.path != "" {
		file, err := os.Open(payload.path)
		if err != nil {
			return nil, err
		}
		source = file
		defer source.Close()
		info, err := source.Stat()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() != expectedSize {
			return nil, fmt.Errorf("hatriecache: backup repository payload %s changed while chunking", payload.name)
		}
	} else if int64(len(payload.data)) != expectedSize {
		return nil, fmt.Errorf("hatriecache: backup repository payload %s size changed while chunking", payload.name)
	}

	chunks := make([]BackupBundleChunk, 0, (expectedSize+int64(chunkSize)-1)/int64(chunkSize))
	wholeHash := sha256.New()
	for offset := int64(0); offset < expectedSize; {
		if err := checkBackupContext(ctx); err != nil {
			return nil, err
		}
		length := int64(chunkSize)
		if remaining := expectedSize - offset; remaining < length {
			length = remaining
		}
		chunkHash := sha256.New()
		var copied int64
		var copyErr error
		if source != nil {
			if _, err := source.Seek(offset, io.SeekStart); err != nil {
				return nil, err
			}
			copied, copyErr = io.CopyN(io.MultiWriter(chunkHash, wholeHash), backupContextReader{ctx: ctx, Reader: source}, length)
			if copyErr != nil {
				return nil, copyErr
			}
		} else {
			end := offset + length
			if offset < 0 || end > int64(len(payload.data)) {
				return nil, fmt.Errorf("hatriecache: backup repository payload %s chunk bounds are invalid", payload.name)
			}
			startIndex, endIndex := int(offset), int(end)
			_, _ = chunkHash.Write(payload.data[startIndex:endIndex])
			_, _ = wholeHash.Write(payload.data[startIndex:endIndex])
			copied = length
		}
		if copied != length {
			return nil, fmt.Errorf("hatriecache: backup repository payload %s chunk size mismatch", payload.name)
		}
		chunks = append(chunks, BackupBundleChunk{
			Offset: offset,
			Size:   length,
			SHA256: hex.EncodeToString(chunkHash.Sum(nil)),
		})
		offset += length
	}
	if got := hex.EncodeToString(wholeHash.Sum(nil)); got != expectedSHA256 {
		return nil, fmt.Errorf("hatriecache: backup repository payload %s checksum changed while chunking", payload.name)
	}
	return chunks, nil
}

func storeBackupRepositoryObjects(ctx context.Context, root string, manifest *BackupBundleManifest, payloads []backupBundlePayloadFile) error {
	files := make(map[string]BackupBundleFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	for _, payload := range payloads {
		if err := checkBackupContext(ctx); err != nil {
			return err
		}
		file, ok := files[payload.name]
		if !ok {
			return fmt.Errorf("hatriecache: backup repository payload %s is undeclared", payload.name)
		}
		if len(file.Chunks) > 0 {
			if err := storeBackupRepositoryChunks(ctx, root, manifest, file, payload); err != nil {
				return err
			}
			continue
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
				_, err = io.Copy(backupContextWriter{ctx: ctx, Writer: writer}, backupContextReader{ctx: ctx, Reader: source})
				return err
			}
			_, err := backupContextWriter{ctx: ctx, Writer: writer}.Write(payload.data)
			return err
		}); err != nil {
			return err
		}
		manifest.NewObjects++
		manifest.NewObjectBytes += file.Size
	}
	return nil
}

func storeBackupRepositoryChunks(ctx context.Context, root string, manifest *BackupBundleManifest, file BackupBundleFile, payload backupBundlePayloadFile) error {
	for _, chunk := range file.Chunks {
		if err := checkBackupContext(ctx); err != nil {
			return err
		}
		objectPath, err := backupRepositoryObjectPath(root, chunk.SHA256)
		if err != nil {
			return err
		}
		if info, err := os.Stat(objectPath); err == nil {
			if !info.Mode().IsRegular() || info.Size() != chunk.Size {
				return fmt.Errorf("hatriecache: backup repository object size mismatch for %s", chunk.SHA256)
			}
			manifest.ReusedObjects++
			manifest.ReusedObjectBytes += chunk.Size
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := writeBackupRepositoryChunk(ctx, objectPath, payload, chunk); err != nil {
			return err
		}
		manifest.NewObjects++
		manifest.NewObjectBytes += chunk.Size
	}
	return nil
}

func writeBackupRepositoryChunk(ctx context.Context, objectPath string, payload backupBundlePayloadFile, chunk BackupBundleChunk) error {
	return writeFileAtomicStream(objectPath, func(writer io.Writer) error {
		if err := checkBackupContext(ctx); err != nil {
			return err
		}
		chunkHash := sha256.New()
		output := io.MultiWriter(backupContextWriter{ctx: ctx, Writer: writer}, chunkHash)
		if payload.path != "" {
			source, err := os.Open(payload.path)
			if err != nil {
				return err
			}
			defer source.Close()
			if _, err := source.Seek(chunk.Offset, io.SeekStart); err != nil {
				return err
			}
			copied, err := io.CopyN(output, backupContextReader{ctx: ctx, Reader: source}, chunk.Size)
			if err != nil {
				return err
			}
			if copied != chunk.Size {
				return fmt.Errorf("hatriecache: backup repository object %s size mismatch", chunk.SHA256)
			}
		} else {
			end := chunk.Offset + chunk.Size
			if chunk.Offset < 0 || end > int64(len(payload.data)) {
				return fmt.Errorf("hatriecache: backup repository payload %s chunk bounds are invalid", payload.name)
			}
			startIndex, endIndex := int(chunk.Offset), int(end)
			if _, err := output.Write(payload.data[startIndex:endIndex]); err != nil {
				return err
			}
		}
		if got := hex.EncodeToString(chunkHash.Sum(nil)); got != chunk.SHA256 {
			return fmt.Errorf("hatriecache: backup repository chunk checksum mismatch for %s", payload.name)
		}
		return nil
	})
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

func validateBackupRepositoryManifestChunks(manifest BackupBundleManifest) error {
	if manifest.RepositoryChunkSize != 0 && (manifest.RepositoryChunkSize < backupRepositoryMinChunkSize || manifest.RepositoryChunkSize > backupRepositoryMaxChunkSize) {
		return errors.New("hatriecache: invalid backup repository manifest chunk size")
	}
	for _, file := range manifest.Files {
		if len(file.Chunks) == 0 {
			continue
		}
		if file.Size <= 0 {
			return fmt.Errorf("hatriecache: backup repository file %s has chunks but no payload", file.Path)
		}
		var offset int64
		for index, chunk := range file.Chunks {
			if chunk.Offset != offset || chunk.Size <= 0 || chunk.Size > file.Size-offset {
				return fmt.Errorf("hatriecache: backup repository file %s has invalid chunk %d bounds", file.Path, index)
			}
			if len(chunk.SHA256) != sha256.Size*2 {
				return fmt.Errorf("hatriecache: backup repository file %s has invalid chunk %d checksum", file.Path, index)
			}
			if _, err := hex.DecodeString(chunk.SHA256); err != nil || strings.ToLower(chunk.SHA256) != chunk.SHA256 {
				return fmt.Errorf("hatriecache: backup repository file %s has invalid chunk %d checksum", file.Path, index)
			}
			offset += chunk.Size
		}
		if offset != file.Size {
			return fmt.Errorf("hatriecache: backup repository file %s chunks do not cover the payload", file.Path)
		}
	}
	return nil
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
	if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := validateBackupRepositoryManifestChunks(manifest); err != nil {
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
			if len(file.Chunks) > 0 {
				if err := restoreBackupRepositoryChunkedFile(root, target, file, true); err != nil {
					return BackupBundleManifest{}, err
				}
				continue
			}
			if err := copyBackupRepositoryObjectWithResume(objectPath, target, file, true); err != nil {
				return BackupBundleManifest{}, err
			}
		}
		return manifest, nil
	}

	files := make([]hatBackup.RestoreFile, 0, len(manifest.Files))
	chunkedFiles := make([]struct {
		file   BackupBundleFile
		target string
	}, 0)
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
		if len(file.Chunks) > 0 {
			chunkedFiles = append(chunkedFiles, struct {
				file   BackupBundleFile
				target string
			}{file: file, target: target})
			continue
		}
		files = append(files, hatBackup.RestoreFile{Source: objectPath, Destination: target, Size: file.Size, SHA256: file.SHA256})
	}
	if err := hatBackup.CopyRestoreFiles(files, fileOptions); err != nil {
		return BackupBundleManifest{}, err
	}
	for _, chunked := range chunkedFiles {
		if err := restoreBackupRepositoryChunkedFile(root, chunked.target, chunked.file, false); err != nil {
			return BackupBundleManifest{}, err
		}
	}
	return manifest, nil
}

func restoreBackupRepositoryChunkedFile(root string, targetPath string, declaration BackupBundleFile, resume bool) (err error) {
	if err := rejectRestoreSymlinkComponents(filepath.Dir(targetPath)); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_WRONLY
	if resume {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	target, err := os.OpenFile(targetPath, flags, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := target.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(targetPath)
		}
	}()

	wholeHash := sha256.New()
	var total int64
	for index, chunk := range declaration.Chunks {
		objectPath, objectErr := backupRepositoryObjectPath(root, chunk.SHA256)
		if objectErr != nil {
			return objectErr
		}
		info, statErr := os.Stat(objectPath)
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() || info.Size() != chunk.Size {
			return fmt.Errorf("hatriecache: backup repository chunk object %d size mismatch for %s", index, declaration.Path)
		}
		source, openErr := os.Open(objectPath)
		if openErr != nil {
			return openErr
		}
		chunkHash := sha256.New()
		copied, copyErr := io.CopyN(io.MultiWriter(target, chunkHash, wholeHash), source, chunk.Size)
		closeErr := source.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if copied != chunk.Size || hex.EncodeToString(chunkHash.Sum(nil)) != chunk.SHA256 {
			return fmt.Errorf("hatriecache: backup repository chunk checksum mismatch for %s", declaration.Path)
		}
		total += copied
	}
	if total != declaration.Size || hex.EncodeToString(wholeHash.Sum(nil)) != declaration.SHA256 {
		return fmt.Errorf("hatriecache: backup repository file checksum mismatch for %s", declaration.Path)
	}
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
			if len(file.Chunks) == 0 {
				if _, seen := reachable[file.SHA256]; seen {
					continue
				}
				reachable[file.SHA256] = struct{}{}
				if file.Size > 0 && total <= int64(^uint64(0)>>1)-file.Size {
					total += file.Size
				} else if file.Size > 0 {
					return int64(^uint64(0) >> 1)
				}
				continue
			}
			for _, chunk := range file.Chunks {
				if _, seen := reachable[chunk.SHA256]; seen {
					continue
				}
				reachable[chunk.SHA256] = struct{}{}
				if chunk.Size > 0 && total <= int64(^uint64(0)>>1)-chunk.Size {
					total += chunk.Size
				} else if chunk.Size > 0 {
					return int64(^uint64(0) >> 1)
				}
			}
		}
	}
	return total
}
