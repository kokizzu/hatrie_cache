package hatCache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrReadOnlyBackupClosed reports an operation attempted after an attachment
// has been closed.
var ErrReadOnlyBackupClosed = errors.New("hatriecache: read-only backup is closed")

// ReadOnlyBackupOptions controls how OpenReadOnlyBackup locates and opens a
// backup. The attachment never publishes a writable data directory.
type ReadOnlyBackupOptions struct {
	// BackupID selects a manifest from an incremental repository. Empty uses
	// the repository's latest pointer.
	BackupID string
	// StorageFormat overrides the format for a raw Pebble checkpoint directory.
	// A backup manifest's format takes precedence when present.
	StorageFormat StorageFormat
	// TempDir is used for verified extraction/materialization of archive and
	// repository inputs. Empty uses the process temporary directory.
	TempDir string
}

// ReadOnlyBackup is an immutable query view of a snapshot or Pebble backup.
// It exposes reads and SQL queries only; no writable HatTrie or persistent
// store handle is returned to callers.
type ReadOnlyBackup struct {
	mu          sync.RWMutex
	trie        *HatTrie
	store       *PebbleStore
	manifest    BackupBundleManifest
	cleanupPath string
	closed      bool
}

type readOnlyBackupSource struct {
	manifest    BackupBundleManifest
	snapshot    string
	store       string
	cleanupPath string
}

// OpenReadOnlyBackup opens a snapshot bundle, Pebble checkpoint bundle,
// incremental repository, raw snapshot file, or native Pebble checkpoint
// directory for read-only queries. Archive/repository inputs are verified and
// staged below a private temporary directory; the caller's data directory is
// never modified.
func OpenReadOnlyBackup(path string, options ReadOnlyBackupOptions) (*ReadOnlyBackup, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("hatriecache: read-only backup path is required")
	}
	source, err := prepareReadOnlyBackupSource(path, options)
	if err != nil {
		return nil, err
	}
	trie := CreateHatTrie()
	attachment := &ReadOnlyBackup{
		trie:        trie,
		manifest:    source.manifest,
		cleanupPath: source.cleanupPath,
	}
	if source.snapshot != "" {
		if _, err := trie.LoadSnapshotWithMetadata(source.snapshot); err != nil {
			_ = attachment.Close()
			return nil, fmt.Errorf("hatriecache: open read-only snapshot: %w", err)
		}
		return attachment, nil
	}

	format, err := readOnlyBackupStorageFormat(source.manifest, options.StorageFormat)
	if err != nil {
		_ = attachment.Close()
		return nil, err
	}
	store, err := openPebbleStoreReadOnlyWithFormat(source.store, format)
	if err != nil {
		_ = attachment.Close()
		return nil, fmt.Errorf("hatriecache: open read-only Pebble backup: %w", err)
	}
	if _, err := store.Load(trie); err != nil {
		_ = store.Close()
		_ = attachment.Close()
		return nil, fmt.Errorf("hatriecache: load read-only Pebble backup: %w", err)
	}
	attachment.store = store
	return attachment, nil
}

// Manifest returns the verified backup metadata used to create the view.
func (attachment *ReadOnlyBackup) Manifest() BackupBundleManifest {
	if attachment == nil {
		return BackupBundleManifest{}
	}
	attachment.mu.RLock()
	defer attachment.mu.RUnlock()
	return attachment.manifest
}

// GetChecked returns the compact value stored at key.
func (attachment *ReadOnlyBackup) GetChecked(key string) (HatValue, error) {
	trie, unlock, err := attachment.lockReadTrie()
	if err != nil {
		return HatValue{}, err
	}
	defer unlock()
	return trie.GetChecked(key)
}

// Get returns the compact value stored at key, or the zero value when the
// attachment is closed or the key is absent.
func (attachment *ReadOnlyBackup) Get(key string) HatValue {
	value, _ := attachment.GetChecked(key)
	return value
}

// GetStringChecked returns a string value and whether the key has a string
// value.
func (attachment *ReadOnlyBackup) GetStringChecked(key string) (string, bool, error) {
	trie, unlock, err := attachment.lockReadTrie()
	if err != nil {
		return "", false, err
	}
	defer unlock()
	return trie.GetStringChecked(key)
}

// GetString returns a string value, or an empty string when the attachment is
// closed or the key is absent or has another type.
func (attachment *ReadOnlyBackup) GetString(key string) string {
	value, _, _ := attachment.GetStringChecked(key)
	return value
}

// GetBytesChecked returns a byte value and whether the key has a byte value.
func (attachment *ReadOnlyBackup) GetBytesChecked(key string) ([]byte, error) {
	trie, unlock, err := attachment.lockReadTrie()
	if err != nil {
		return nil, err
	}
	defer unlock()
	return trie.GetBytesChecked(key)
}

// KeysWithPrefixChecked returns non-expired keys matching prefix in the
// attached immutable view.
func (attachment *ReadOnlyBackup) KeysWithPrefixChecked(prefix string, sorted bool) ([]string, error) {
	trie, unlock, err := attachment.lockReadTrie()
	if err != nil {
		return nil, err
	}
	defer unlock()
	return trie.KeysWithPrefixChecked(prefix, sorted)
}

// KeysWithPrefix returns non-expired keys matching prefix. It returns an empty
// list when the attachment is closed or prefix validation fails.
func (attachment *ReadOnlyBackup) KeysWithPrefix(prefix string, sorted bool) []string {
	keys, _ := attachment.KeysWithPrefixChecked(prefix, sorted)
	return keys
}

// QuerySQL executes a read-only relational query against the attached backup.
// Mutation SQL is not exposed by this type.
func (attachment *ReadOnlyBackup) QuerySQL(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	trie, unlock, err := attachment.lockReadTrie()
	if err != nil {
		return SQLQueryResult{}, err
	}
	defer unlock()
	return ExecuteSQLQueryParameters(ctx, source, trie, parameters, options)
}

// Close releases the immutable view and any temporary materialization. Close
// is idempotent.
func (attachment *ReadOnlyBackup) Close() error {
	if attachment == nil {
		return nil
	}
	attachment.mu.Lock()
	if attachment.closed {
		attachment.mu.Unlock()
		return nil
	}
	attachment.closed = true
	trie := attachment.trie
	store := attachment.store
	cleanupPath := attachment.cleanupPath
	attachment.trie = nil
	attachment.store = nil
	attachment.cleanupPath = ""
	attachment.mu.Unlock()

	var errs []error
	if store != nil {
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if trie != nil {
		trie.Destroy()
	}
	if cleanupPath != "" {
		if err := os.RemoveAll(cleanupPath); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (attachment *ReadOnlyBackup) lockReadTrie() (*HatTrie, func(), error) {
	if attachment == nil {
		return nil, func() {}, ErrReadOnlyBackupClosed
	}
	attachment.mu.RLock()
	if attachment.closed || attachment.trie == nil {
		attachment.mu.RUnlock()
		return nil, func() {}, ErrReadOnlyBackupClosed
	}
	return attachment.trie, attachment.mu.RUnlock, nil
}

func prepareReadOnlyBackupSource(path string, options ReadOnlyBackupOptions) (readOnlyBackupSource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return readOnlyBackupSource{}, err
	}
	if !info.IsDir() {
		manifest, manifestErr := readBackupBundleManifest(path)
		if manifestErr == nil {
			workDir, err := makeReadOnlyBackupTempDir(options.TempDir)
			if err != nil {
				return readOnlyBackupSource{}, err
			}
			if err := extractBackupBundleFiles(path, workDir, manifest.Files); err != nil {
				_ = os.RemoveAll(workDir)
				return readOnlyBackupSource{}, err
			}
			source, err := readOnlyBackupSourceFromRoot(workDir, manifest)
			if err != nil {
				_ = os.RemoveAll(workDir)
				return readOnlyBackupSource{}, err
			}
			source.cleanupPath = workDir
			return source, nil
		}
		return readOnlyBackupSource{
			manifest: BackupBundleManifest{
				Version:        BackupBundleVersion,
				Mode:           BackupModeSnapshot,
				Snapshot:       path,
				SnapshotFormat: string(DefaultSnapshotFormat),
			},
			snapshot: path,
		}, nil
	}

	if _, err := os.Stat(filepath.Join(path, backupRepositoryDescriptorPath)); err == nil {
		if err := verifyBackupRepositoryDescriptor(path); err != nil {
			return readOnlyBackupSource{}, err
		}
		manifest, err := readBackupRepositoryManifest(path, options.BackupID)
		if err != nil {
			return readOnlyBackupSource{}, err
		}
		workDir, err := makeReadOnlyBackupTempDir(options.TempDir)
		if err != nil {
			return readOnlyBackupSource{}, err
		}
		materialized := filepath.Join(workDir, "materialized")
		if _, err := materializeBackupRepositoryWithConcurrency(path, manifest.BackupID, materialized, false, 0); err != nil {
			_ = os.RemoveAll(workDir)
			return readOnlyBackupSource{}, err
		}
		source, err := readOnlyBackupSourceFromRoot(materialized, manifest)
		if err != nil {
			_ = os.RemoveAll(workDir)
			return readOnlyBackupSource{}, err
		}
		source.cleanupPath = workDir
		return source, nil
	}

	if _, err := os.Stat(filepath.Join(path, "snapshot.hc")); err == nil {
		return readOnlyBackupSource{
			manifest: BackupBundleManifest{Version: BackupBundleVersion, Mode: BackupModeSnapshot, Snapshot: "snapshot.hc", SnapshotFormat: string(DefaultSnapshotFormat)},
			snapshot: filepath.Join(path, "snapshot.hc"),
		}, nil
	}

	if _, err := os.Stat(filepath.Join(path, "CURRENT")); err == nil {
		return readOnlyBackupSource{
			manifest: BackupBundleManifest{Version: BackupBundleVersion, Mode: BackupModePebbleCheckpoint, Store: path, StorageBackend: string(StorageBackendPebble)},
			store:    path,
		}, nil
	}
	checkpointPath := filepath.Join(path, backupBundleStorePath)
	if _, err := os.Stat(filepath.Join(checkpointPath, "CURRENT")); err == nil {
		return readOnlyBackupSource{
			manifest: BackupBundleManifest{Version: BackupBundleVersion, Mode: BackupModePebbleCheckpoint, Store: backupBundleStorePath, StorageBackend: string(StorageBackendPebble)},
			store:    checkpointPath,
		}, nil
	}

	return readOnlyBackupSource{}, fmt.Errorf("hatriecache: unsupported read-only backup directory %q", path)
}

func readOnlyBackupSourceFromRoot(root string, manifest BackupBundleManifest) (readOnlyBackupSource, error) {
	switch backupBundleManifestMode(manifest) {
	case BackupModeSnapshot:
		if manifest.Snapshot == "" {
			return readOnlyBackupSource{}, errors.New("hatriecache: read-only backup manifest missing snapshot")
		}
		relative, err := cleanBackupBundlePath(manifest.Snapshot)
		if err != nil {
			return readOnlyBackupSource{}, err
		}
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := requireRegularReadOnlyBackupFile(path); err != nil {
			return readOnlyBackupSource{}, err
		}
		return readOnlyBackupSource{manifest: manifest, snapshot: path}, nil
	case BackupModePebbleCheckpoint, BackupModePebbleIncremental:
		if manifest.Store == "" || manifest.StorageBackend != "" && manifest.StorageBackend != string(StorageBackendPebble) {
			return readOnlyBackupSource{}, errors.New("hatriecache: read-only backup is not a Pebble store")
		}
		relative, err := cleanBackupBundlePath(manifest.Store)
		if err != nil {
			return readOnlyBackupSource{}, err
		}
		path := filepath.Join(root, filepath.FromSlash(relative))
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			if err != nil {
				return readOnlyBackupSource{}, err
			}
			return readOnlyBackupSource{}, fmt.Errorf("hatriecache: read-only Pebble store path %q is not a directory", path)
		}
		return readOnlyBackupSource{manifest: manifest, store: path}, nil
	default:
		return readOnlyBackupSource{}, fmt.Errorf("hatriecache: unsupported read-only backup mode %q", manifest.Mode)
	}
}

func readOnlyBackupStorageFormat(manifest BackupBundleManifest, override StorageFormat) (StorageFormat, error) {
	if manifest.StorageFormat != "" {
		return ParseStorageFormat(manifest.StorageFormat)
	}
	if override != "" {
		return ParseStorageFormat(string(override))
	}
	return DefaultStorageFormat, nil
}

func makeReadOnlyBackupTempDir(base string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		base = os.TempDir()
	}
	return os.MkdirTemp(base, "hatrie-readonly-backup-*")
}

func requireRegularReadOnlyBackupFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("hatriecache: read-only backup payload %q is not a regular file", path)
	}
	return nil
}
