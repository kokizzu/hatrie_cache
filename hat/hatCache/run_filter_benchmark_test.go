package hatCache

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
)

func TestPersistentStoreRunFilterPreservesEntrySemantics(t *testing.T) {
	for _, bitsPerKey := range []int{0, 10} {
		t.Run(fmt.Sprintf("bits-%d", bitsPerKey), func(t *testing.T) {
			configureTT017BloomFilter(t, bitsPerKey)
			for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
				t.Run(string(backend), func(t *testing.T) {
					store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), backend, StorageFormatBinary)
					if err != nil {
						t.Fatal(err)
					}
					defer store.Close()
					trie := CreateHatTrie()
					defer trie.Destroy()
					trie.UpsertString("present", "value")
					if err := store.Save(trie); err != nil {
						t.Fatal(err)
					}
					if _, ok, err := entryForTT017(store, "present"); err != nil || !ok {
						t.Fatalf("Entry(present) = %v, %v, want present entry", ok, err)
					}
					if _, ok, err := entryForTT017(store, "missing"); err != nil || ok {
						t.Fatalf("Entry(missing) = %v, %v, want clean miss", ok, err)
					}
				})
			}
		})
	}
}

func TestPersistentStoreRunFilterReopenPreservesEntrySemantics(t *testing.T) {
	for _, bitsPerKey := range []int{0, 10} {
		t.Run(fmt.Sprintf("bits-%d", bitsPerKey), func(t *testing.T) {
			configureTT017BloomFilter(t, bitsPerKey)
			for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
				t.Run(string(backend), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "cache")
					store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
					if err != nil {
						t.Fatal(err)
					}
					trie := CreateHatTrie()
					trie.UpsertString("present", "value")
					if err := store.Save(trie); err != nil {
						trie.Destroy()
						store.Close()
						t.Fatal(err)
					}
					trie.Destroy()
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.Close()
					if _, ok, err := entryForTT017(reopened, "present"); err != nil || !ok {
						t.Fatalf("reopened Entry(present) = %v, %v, want present entry", ok, err)
					}
					if _, ok, err := entryForTT017(reopened, "missing"); err != nil || ok {
						t.Fatalf("reopened Entry(missing) = %v, %v, want clean miss", ok, err)
					}
				})
			}
		})
	}
}

func TestTT017PersistentStoreFilterStorageFootprint(t *testing.T) {
	for _, bitsPerKey := range []int{0, 10} {
		t.Run(fmt.Sprintf("bits-%d", bitsPerKey), func(t *testing.T) {
			configureTT017BloomFilter(t, bitsPerKey)
			for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
				t.Run(string(backend), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "cache")
					store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
					if err != nil {
						t.Fatal(err)
					}
					trie := CreateHatTrie()
					defer trie.Destroy()
					for index := 0; index < 4096; index++ {
						trie.UpsertString(fmt.Sprintf("key-%04d", index), "value")
					}
					if err := store.Save(trie); err != nil {
						t.Fatal(err)
					}
					if _, err := store.Compact(LevelDBCompactionOptions{}); err != nil {
						store.Close()
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					bytes, err := directoryBytesTT017(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("compacted storage bytes=%d", bytes)
				})
			}
		})
	}
}

func TestPersistentStoreBloomFilterConfiguration(t *testing.T) {
	previous := PersistentStoreBloomFilterBitsPerKey()
	t.Cleanup(func() {
		if err := ConfigurePersistentStoreBloomFilterBitsPerKey(previous); err != nil {
			t.Errorf("restore bloom filter bits/key: %v", err)
		}
	})
	if err := ConfigurePersistentStoreBloomFilterBitsPerKey(DefaultPersistentStoreBloomFilterBitsPerKey); err != nil {
		t.Fatal(err)
	}
	if got := PersistentStoreBloomFilterBitsPerKey(); got != DefaultPersistentStoreBloomFilterBitsPerKey {
		t.Fatalf("default bloom filter bits/key = %d, want %d", got, DefaultPersistentStoreBloomFilterBitsPerKey)
	}
	if err := ConfigurePersistentStoreBloomFilterBitsPerKey(-1); err == nil {
		t.Fatal("negative bloom filter bits/key unexpectedly accepted")
	}
	if err := ConfigurePersistentStoreBloomFilterBitsPerKey(MaxPersistentStoreBloomFilterBitsPerKey + 1); err == nil {
		t.Fatal("excessive bloom filter bits/key unexpectedly accepted")
	}
	if err := ConfigurePersistentStoreBloomFilterBitsPerKey(10); err != nil {
		t.Fatal(err)
	}
	if got := PersistentStoreBloomFilterBitsPerKey(); got != 10 {
		t.Fatalf("configured bloom filter bits/key = %d, want 10", got)
	}
}

func BenchmarkTT017PersistentStoreEntry(b *testing.B) {
	previous := PersistentStoreBloomFilterBitsPerKey()
	defer func() {
		_ = ConfigurePersistentStoreBloomFilterBitsPerKey(previous)
	}()
	for _, benchmark := range []struct {
		backend    StorageBackend
		bitsPerKey int
		key        string
	}{
		{backend: StorageBackendLevelDB, bitsPerKey: 0, key: "missing"},
		{backend: StorageBackendLevelDB, bitsPerKey: 0, key: "key-2048"},
		{backend: StorageBackendLevelDB, bitsPerKey: 10, key: "missing"},
		{backend: StorageBackendLevelDB, bitsPerKey: 10, key: "key-2048"},
		{backend: StorageBackendPebble, bitsPerKey: 0, key: "missing"},
		{backend: StorageBackendPebble, bitsPerKey: 0, key: "key-2048"},
		{backend: StorageBackendPebble, bitsPerKey: 10, key: "missing"},
		{backend: StorageBackendPebble, bitsPerKey: 10, key: "key-2048"},
	} {
		name := "miss"
		if benchmark.key != "missing" {
			name = "hit"
		}
		b.Run(fmt.Sprintf("%s/bits-%d/%s", benchmark.backend, benchmark.bitsPerKey, name), func(b *testing.B) {
			if err := ConfigurePersistentStoreBloomFilterBitsPerKey(benchmark.bitsPerKey); err != nil {
				b.Fatal(err)
			}
			store, err := OpenPersistentStoreWithFormat(filepath.Join(b.TempDir(), "cache"), benchmark.backend, StorageFormatBinary)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			trie := CreateHatTrie()
			defer trie.Destroy()
			for index := 0; index < 4096; index++ {
				trie.UpsertString(fmt.Sprintf("key-%04d", index), "value")
			}
			if err := store.Save(trie); err != nil {
				b.Fatal(err)
			}
			if _, err := store.Compact(LevelDBCompactionOptions{}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _, err := entryForTT017(store, benchmark.key)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func entryForTT017(store PersistentStore, key string) (snapshotEntry, bool, error) {
	switch value := store.(type) {
	case *LevelDBStore:
		return value.Entry(key)
	case *PebbleStore:
		return value.Entry(key)
	default:
		return snapshotEntry{}, false, fmt.Errorf("unsupported persistent store %T", store)
	}
}

func configureTT017BloomFilter(t *testing.T, bitsPerKey int) {
	t.Helper()
	previous := PersistentStoreBloomFilterBitsPerKey()
	if err := ConfigurePersistentStoreBloomFilterBitsPerKey(bitsPerKey); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ConfigurePersistentStoreBloomFilterBitsPerKey(previous); err != nil {
			t.Errorf("restore bloom filter bits/key: %v", err)
		}
	})
}

func directoryBytesTT017(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
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
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
