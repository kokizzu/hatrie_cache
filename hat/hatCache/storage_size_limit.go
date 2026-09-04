package hatCache

import (
	"errors"
	"fmt"
)

// DefaultPersistentStoreMaxBytes disables the durable logical-size limit.
const DefaultPersistentStoreMaxBytes int64 = 0

var ErrPersistentStorageSizeLimitExceeded = errors.New("hatriecache: persistent storage logical-size limit exceeded")

// ConfigurePersistentStoreMaxBytes sets the maximum logical serialized size
// accepted by future persistent-store save operations. Zero disables the
// limit. The limit is measured as cache-key bytes plus encoded value bytes;
// LSM metadata, compaction amplification, and encryption framing are not
// included.
func ConfigurePersistentStoreMaxBytes(store PersistentStore, maxBytes int64) error {
	if maxBytes < 0 {
		return errors.New("hatriecache: persistent storage max bytes must be non-negative")
	}
	if store == nil {
		if maxBytes == 0 {
			return nil
		}
		return errors.New("hatriecache: persistent storage max bytes requires a store")
	}
	switch value := store.(type) {
	case *LevelDBStore:
		value.storageSizeLimitBytes.Store(maxBytes)
	case *PebbleStore:
		value.storageSizeLimitBytes.Store(maxBytes)
	default:
		return fmt.Errorf("hatriecache: persistent storage max bytes is unsupported by backend %q", store.Backend())
	}
	return nil
}

// EstimatePersistentStorageBytes returns the logical serialized bytes for all
// current trie entries using format. It includes cold persistent references by
// reading their source records, so it remains an estimate of the full durable
// dataset rather than only currently materialized cache values.
func (trie *HatTrie) EstimatePersistentStorageBytes(format StorageFormat) (int64, error) {
	if trie == nil {
		return 0, ErrNilHatTrie
	}
	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return 0, err
	}
	var total int64
	err = trie.scanLevelDBEntryDataForStore(nil, nil, format, func(key string, data []byte) error {
		keyBytes := int64(len(key))
		valueBytes := int64(len(data))
		if keyBytes > maxPersistentStorageEstimateBytes-valueBytes {
			return errors.New("hatriecache: persistent storage logical-size estimate overflow")
		}
		entryBytes := keyBytes + valueBytes
		if total > maxPersistentStorageEstimateBytes-entryBytes {
			return errors.New("hatriecache: persistent storage logical-size estimate overflow")
		}
		total += entryBytes
		return nil
	})
	return total, err
}

const maxPersistentStorageEstimateBytes = int64(1<<63 - 1)

func checkPersistentStorageSizeLimit(trie *HatTrie, format StorageFormat, maxBytes int64, backend StorageBackend) error {
	if maxBytes <= 0 {
		return nil
	}
	estimated, err := trie.EstimatePersistentStorageBytes(format)
	if err != nil {
		return err
	}
	if estimated > maxBytes {
		return fmt.Errorf("%w: backend=%s estimated=%d max=%d", ErrPersistentStorageSizeLimitExceeded, backend, estimated, maxBytes)
	}
	return nil
}

func (store *LevelDBStore) SetStorageSizeLimitBytes(maxBytes int64) error {
	if store == nil {
		return errors.New("hatriecache: leveldb store is nil")
	}
	if maxBytes < 0 {
		return errors.New("hatriecache: persistent storage max bytes must be non-negative")
	}
	store.storageSizeLimitBytes.Store(maxBytes)
	return nil
}

func (store *LevelDBStore) StorageSizeLimitBytes() int64 {
	if store == nil {
		return 0
	}
	return store.storageSizeLimitBytes.Load()
}

func (store *PebbleStore) SetStorageSizeLimitBytes(maxBytes int64) error {
	if store == nil {
		return errors.New("hatriecache: pebble store is nil")
	}
	if maxBytes < 0 {
		return errors.New("hatriecache: persistent storage max bytes must be non-negative")
	}
	store.storageSizeLimitBytes.Store(maxBytes)
	return nil
}

func (store *PebbleStore) StorageSizeLimitBytes() int64 {
	if store == nil {
		return 0
	}
	return store.storageSizeLimitBytes.Load()
}
