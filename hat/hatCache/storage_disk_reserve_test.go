package hatCache

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
)

func TestPersistentStoreDiskReserveRejectsWritesWhenReserveExceedsFreeSpace(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), backend, StorageFormatBinary)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			trie := CreateHatTrie()
			defer trie.Destroy()
			trie.UpsertString("key", "value")
			if err := ConfigurePersistentStoreDiskReserveBytes(store, math.MaxInt64); err != nil {
				t.Fatalf("ConfigurePersistentStoreDiskReserveBytes() error = %v", err)
			}
			if err := store.Save(trie); !errors.Is(err, ErrPersistentStorageDiskReserveExceeded) {
				t.Fatalf("Save() error = %v, want ErrPersistentStorageDiskReserveExceeded", err)
			}
		})
	}
}

func TestPersistentStoreDiskReserveZeroKeepsDefaultBehavior(t *testing.T) {
	store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), StorageBackendPebble, StorageFormatBinary)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("key", "value")
	if err := ConfigurePersistentStoreDiskReserveBytes(store, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save() error = %v, want default behavior", err)
	}
}

func TestPersistentStoreDiskReserveValidatesConfiguration(t *testing.T) {
	store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), StorageBackendPebble, StorageFormatBinary)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := ConfigurePersistentStoreDiskReserveBytes(store, -1); err == nil {
		t.Fatal("negative disk reserve unexpectedly accepted")
	}
	if err := ConfigurePersistentStoreDiskReserveBytes(store, 1); err != nil {
		t.Fatalf("positive disk reserve error = %v", err)
	}
	if got := PersistentStoreDiskReserveBytes(store); got != 1 {
		t.Fatalf("PersistentStoreDiskReserveBytes() = %d, want 1", got)
	}
}

func TestPersistentStoreDiskReserveCoversKeyAndDirtySaves(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), backend, StorageFormatBinary)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			trie := CreateHatTrie()
			defer trie.Destroy()
			trie.UpsertString("key", "initial")
			if err := store.Save(trie); err != nil {
				t.Fatalf("initial Save() error = %v", err)
			}
			if err := ConfigurePersistentStoreDiskReserveBytes(store, math.MaxInt64); err != nil {
				t.Fatal(err)
			}
			trie.UpsertString("key", "changed")
			if err := store.SaveKeys(trie, []string{"key"}); !errors.Is(err, ErrPersistentStorageDiskReserveExceeded) {
				t.Fatalf("SaveKeys() error = %v, want ErrPersistentStorageDiskReserveExceeded", err)
			}
			trie.UpsertString("dirty", "value")
			tracker := NewLevelDBDirtyTracker()
			tracker.Mark("dirty")
			if err := store.SaveDirty(trie, tracker); !errors.Is(err, ErrPersistentStorageDiskReserveExceeded) {
				t.Fatalf("SaveDirty() error = %v, want ErrPersistentStorageDiskReserveExceeded", err)
			}
			if tracker.Pending() != 1 {
				t.Fatalf("dirty tracker pending = %d, want 1 after rejected save", tracker.Pending())
			}
		})
	}
}
