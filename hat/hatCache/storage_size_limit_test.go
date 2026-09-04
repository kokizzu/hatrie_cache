package hatCache

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPersistentStoreMaxBytesRejectsBeforeCommit(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		for _, format := range []StorageFormat{StorageFormatBinary, StorageFormatJSON} {
			t.Run(string(backend)+"/"+string(format), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "cache")
				store, err := OpenPersistentStoreWithFormat(path, backend, format)
				if err != nil {
					t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
				}
				defer store.Close()

				trie := CreateHatTrie()
				defer trie.Destroy()
				trie.UpsertString("key", "value")
				estimated, err := trie.EstimatePersistentStorageBytes(format)
				if err != nil {
					t.Fatalf("EstimatePersistentStorageBytes() error = %v", err)
				}
				if estimated < 2 {
					t.Fatalf("estimated persistent bytes = %d, want at least 2", estimated)
				}
				if err := ConfigurePersistentStoreMaxBytes(store, estimated-1); err != nil {
					t.Fatalf("ConfigurePersistentStoreMaxBytes() error = %v", err)
				}
				if err := store.Save(trie); !errors.Is(err, ErrPersistentStorageSizeLimitExceeded) {
					t.Fatalf("Save() error = %v, want ErrPersistentStorageSizeLimitExceeded", err)
				}
				if err := store.Close(); err != nil {
					t.Fatalf("close store before reopen: %v", err)
				}
				loadedStore, err := OpenPersistentStoreWithFormat(path, backend, format)
				if err != nil {
					t.Fatalf("reopen store error = %v", err)
				}
				defer loadedStore.Close()
				loaded := CreateHatTrie()
				defer loaded.Destroy()
				if count, err := loadedStore.Load(loaded); err != nil {
					t.Fatalf("Load() error = %v", err)
				} else if count != 0 || loaded.Exists("key") {
					t.Fatalf("failed save committed data: count=%d exists=%v", count, loaded.Exists("key"))
				}
			})
		}
	}
}

func TestPersistentStoreMaxBytesZeroKeepsDefaultBehavior(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPersistentStoreWithFormat(path, StorageBackendPebble, DefaultStorageFormat)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
	}
	defer store.Close()
	if err := ConfigurePersistentStoreMaxBytes(store, 0); err != nil {
		t.Fatalf("ConfigurePersistentStoreMaxBytes(0) error = %v", err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("key", "value")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save() error = %v, want default unlimited behavior", err)
	}
}

func BenchmarkPersistentStorageSizeLimitCheckDisabled(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	for index := 0; index < 128; index++ {
		trie.UpsertString("key-"+strconv.Itoa(index), "value")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := checkPersistentStorageSizeLimit(trie, DefaultStorageFormat, 0, StorageBackendPebble); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersistentStorageSizeLimitCheckEnabled(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	for index := 0; index < 128; index++ {
		trie.UpsertString("key-"+strconv.Itoa(index), "value")
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := checkPersistentStorageSizeLimit(trie, DefaultStorageFormat, 1<<30, StorageBackendPebble); err != nil {
			b.Fatal(err)
		}
	}
}
