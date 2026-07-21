package hatriecache

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// StorageBackend selects the local persistent key/value engine.
type StorageBackend string

const (
	StorageBackendAuto    StorageBackend = "auto"
	StorageBackendPebble  StorageBackend = "pebble"
	StorageBackendLevelDB StorageBackend = "leveldb"
)

// DefaultStorageBackend is used for new paths opened in auto mode.
const DefaultStorageBackend = StorageBackendPebble

const storageBackendMarkerSuffix = ".backend"

type persistentReferenceStore interface {
	Entry(string) (snapshotEntry, bool, error)
	entryData(string) ([]byte, bool, error)
}

// PersistentStore is the common durability and lazy-loading contract supported
// by Pebble and LevelDB. LevelDB-named option/result types remain compatible.
type PersistentStore interface {
	Backend() StorageBackend
	Path() string
	Format() StorageFormat
	Properties() (LevelDBProperties, error)
	Close() error
	Save(*HatTrie) error
	SaveWithJournalSequence(*HatTrie, uint64) error
	SaveKeys(*HatTrie, []string) error
	SaveKeysWithOptions(*HatTrie, []string, LevelDBSaveOptions) error
	SaveDirty(*HatTrie, *LevelDBDirtyTracker) error
	SaveDirtyWithOptions(*HatTrie, *LevelDBDirtyTracker, LevelDBSaveOptions) error
	SaveDirtyWithJournalSequence(*HatTrie, *LevelDBDirtyTracker, LevelDBSaveOptions, uint64) error
	AppliedJournalSequence() (uint64, bool, error)
	Load(*HatTrie) (int, error)
	LoadWithPolicy(*HatTrie, LevelDBLoadPolicy) (LevelDBLoadResult, error)
	Flush(*HatTrie) (LevelDBFlushResult, error)
	SpillCold(*HatTrie, LevelDBSpillOptions) (LevelDBSpillResult, error)
	Compact(LevelDBCompactionOptions) (LevelDBCompactionResult, error)
}

// ParseStorageBackend validates an auto, Pebble, or LevelDB backend name.
func ParseStorageBackend(value string) (StorageBackend, error) {
	switch StorageBackend(strings.ToLower(strings.TrimSpace(value))) {
	case "", StorageBackendAuto:
		return StorageBackendAuto, nil
	case StorageBackendPebble:
		return StorageBackendPebble, nil
	case StorageBackendLevelDB:
		return StorageBackendLevelDB, nil
	default:
		return "", fmt.Errorf("hatriecache: storage backend must be auto, pebble, or leveldb")
	}
}

// OpenPersistentStore opens an auto-detected store with the default codec.
func OpenPersistentStore(path string) (PersistentStore, error) {
	return OpenPersistentStoreWithFormat(path, StorageBackendAuto, DefaultStorageFormat)
}

// OpenPersistentStoreWithFormat opens a store and persists its engine marker.
func OpenPersistentStoreWithFormat(path string, requested StorageBackend, format StorageFormat) (PersistentStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("hatriecache: storage path is required")
	}
	backend, err := resolveStorageBackend(path, requested)
	if err != nil {
		return nil, err
	}
	var store PersistentStore
	switch backend {
	case StorageBackendPebble:
		store, err = OpenPebbleStoreWithFormat(path, format)
	case StorageBackendLevelDB:
		store, err = OpenLevelDBStoreWithFormat(path, format)
	default:
		err = fmt.Errorf("hatriecache: unsupported storage backend %q", backend)
	}
	if err != nil {
		return nil, err
	}
	if err := writeStorageBackendMarker(path, backend); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func resolveStorageBackend(path string, requested StorageBackend) (StorageBackend, error) {
	requested, err := ParseStorageBackend(string(requested))
	if err != nil {
		return "", err
	}
	marked, hasMarker, err := readStorageBackendMarker(path)
	if err != nil {
		return "", err
	}
	if hasMarker {
		if requested != StorageBackendAuto && requested != marked {
			return "", fmt.Errorf("hatriecache: storage backend %q does not match %q marker", requested, marked)
		}
		return marked, nil
	}
	if requested != StorageBackendAuto {
		return requested, nil
	}
	entries, err := os.ReadDir(path)
	if err == nil && len(entries) > 0 {
		return StorageBackendLevelDB, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return DefaultStorageBackend, nil
}

func storageBackendMarkerPath(path string) string {
	return path + storageBackendMarkerSuffix
}

func readStorageBackendMarker(path string) (StorageBackend, bool, error) {
	data, err := os.ReadFile(storageBackendMarkerPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	backend, err := ParseStorageBackend(string(data))
	if err != nil || backend == StorageBackendAuto {
		return "", false, fmt.Errorf("hatriecache: invalid storage backend marker %q", strings.TrimSpace(string(data)))
	}
	return backend, true, nil
}

func writeStorageBackendMarker(path string, backend StorageBackend) error {
	return writeFileAtomic(storageBackendMarkerPath(path), []byte(string(backend)+"\n"))
}

func (store *LevelDBStore) Backend() StorageBackend {
	return StorageBackendLevelDB
}

func (store *LevelDBStore) Path() string {
	if store == nil {
		return ""
	}
	return store.path
}

func (store *LevelDBStore) Format() StorageFormat {
	if store == nil {
		return ""
	}
	return store.format
}

func (store *LevelDBStore) Properties() (LevelDBProperties, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return LevelDBProperties{}, err
	}
	defer unlock()
	return levelDBProperties(db), nil
}
