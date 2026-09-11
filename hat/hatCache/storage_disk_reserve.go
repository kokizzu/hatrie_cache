package hatCache

import (
	"errors"
	"fmt"
)

// DefaultPersistentStoreDiskReserveBytes disables physical free-space
// admission. A positive value keeps at least that many bytes available on the
// filesystem containing the persistent store.
const DefaultPersistentStoreDiskReserveBytes int64 = 0

var (
	ErrPersistentStorageDiskReserveExceeded    = errors.New("hatriecache: persistent storage disk reserve exceeded")
	ErrPersistentStorageDiskReserveUnavailable = errors.New("hatriecache: persistent storage disk reserve unavailable")
)

// ConfigurePersistentStoreDiskReserveBytes sets the minimum free bytes
// required before future persistent-store saves. Zero disables the check. The
// setting is process-local and is not written into the store.
func ConfigurePersistentStoreDiskReserveBytes(store PersistentStore, reserveBytes int64) error {
	if reserveBytes < 0 {
		return errors.New("hatriecache: persistent storage disk reserve must be non-negative")
	}
	if store == nil {
		if reserveBytes == 0 {
			return nil
		}
		return errors.New("hatriecache: persistent storage disk reserve requires a store")
	}
	switch value := store.(type) {
	case *LevelDBStore:
		value.storageDiskReserveBytes.Store(reserveBytes)
	case *PebbleStore:
		value.storageDiskReserveBytes.Store(reserveBytes)
	default:
		return fmt.Errorf("hatriecache: persistent storage disk reserve is unsupported by backend %q", store.Backend())
	}
	return nil
}

// PersistentStoreDiskReserveBytes returns the configured process-local
// physical free-space reserve, or zero for an unsupported or nil store.
func PersistentStoreDiskReserveBytes(store PersistentStore) int64 {
	switch value := store.(type) {
	case *LevelDBStore:
		if value == nil {
			return 0
		}
		return value.storageDiskReserveBytes.Load()
	case *PebbleStore:
		if value == nil {
			return 0
		}
		return value.storageDiskReserveBytes.Load()
	default:
		return 0
	}
}

func checkPersistentStorageDiskReserve(path string, reserveBytes int64, backend StorageBackend) error {
	if reserveBytes <= 0 {
		return nil
	}
	freeBytes, err := persistentStoreAvailableBytes(path)
	if err != nil {
		return fmt.Errorf("%w: backend=%s path=%q: %v", ErrPersistentStorageDiskReserveUnavailable, backend, path, err)
	}
	if freeBytes <= uint64(reserveBytes) {
		return fmt.Errorf("%w: backend=%s path=%q free=%d reserve=%d", ErrPersistentStorageDiskReserveExceeded, backend, path, freeBytes, reserveBytes)
	}
	return nil
}
