package hatriecache

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandDumpColdBinaryEncodingMatchesSnapshot(t *testing.T) {
	testCommandDumpColdBinaryEncodingMatchesSnapshot(t, StorageBackendLevelDB)
}

func TestCommandDumpColdBinaryEncodingMatchesSnapshotPebble(t *testing.T) {
	testCommandDumpColdBinaryEncodingMatchesSnapshot(t, StorageBackendPebble)
}

func testCommandDumpColdBinaryEncodingMatchesSnapshot(t *testing.T, backend StorageBackend) {
	t.Helper()
	source := CreateHatTrie()
	defer source.Destroy()
	if err := source.UpsertCounterChecked("counter", -12345); err != nil {
		t.Fatalf("UpsertCounterChecked() error = %v", err)
	}
	if err := source.UpsertStringChecked("string", "cold-value"); err != nil {
		t.Fatalf("UpsertStringChecked() error = %v", err)
	}
	if err := source.UpsertBytesChecked("bytes", bytes.Repeat([]byte{0, 255, 1, 128}, 1024)); err != nil {
		t.Fatalf("UpsertBytesChecked() error = %v", err)
	}
	if err := source.UpsertMapChecked("map", Map{"nested": Slice{"value", int64(4)}}); err != nil {
		t.Fatalf("UpsertMapChecked() error = %v", err)
	}
	if !source.Expire("map", time.Hour) {
		t.Fatal("Expire(map) = false")
	}

	loaded, store := loadColdCommandDumpPersistentTrie(t, source, backend, StorageFormatBinary)
	defer store.Close()
	defer loaded.Destroy()
	for _, key := range []string{"counter", "string", "bytes", "map"} {
		assertColdLevelDBReference(t, loaded, key)
		assertCommandDumpMatchesSnapshot(t, loaded, key)
	}
}

func TestCommandDumpColdJSONFallsBackToSnapshot(t *testing.T) {
	testCommandDumpColdJSONFallsBackToSnapshot(t, StorageBackendLevelDB)
}

func TestCommandDumpColdJSONFallsBackToSnapshotPebble(t *testing.T) {
	testCommandDumpColdJSONFallsBackToSnapshot(t, StorageBackendPebble)
}

func testCommandDumpColdJSONFallsBackToSnapshot(t *testing.T, backend StorageBackend) {
	t.Helper()
	source := CreateHatTrie()
	defer source.Destroy()
	if err := source.UpsertMapChecked("map", Map{"nested": Slice{"value", int64(4)}}); err != nil {
		t.Fatalf("UpsertMapChecked() error = %v", err)
	}
	if err := source.UpsertBytesChecked("bytes", bytes.Repeat([]byte{0, 255, 1, 128}, DiskBytesThreshold/4)); err != nil {
		t.Fatalf("UpsertBytesChecked() error = %v", err)
	}

	loaded, store := loadColdCommandDumpPersistentTrie(t, source, backend, StorageFormatJSON)
	defer store.Close()
	defer loaded.Destroy()
	for _, key := range []string{"map", "bytes"} {
		assertColdLevelDBReference(t, loaded, key)
		assertCommandDumpMatchesSnapshot(t, loaded, key)
	}
}

func TestCommandDumpColdFixedBinaryEncodingMatchesSnapshot(t *testing.T) {
	testCommandDumpColdFixedBinaryEncodingMatchesSnapshot(t, StorageBackendLevelDB)
}

func TestCommandDumpColdFixedBinaryEncodingMatchesSnapshotPebble(t *testing.T) {
	testCommandDumpColdFixedBinaryEncodingMatchesSnapshot(t, StorageBackendPebble)
}

func testCommandDumpColdFixedBinaryEncodingMatchesSnapshot(t *testing.T, backend StorageBackend) {
	t.Helper()
	source := CreateHatTrie()
	defer source.Destroy()
	requests := []CacheCommandRequest{
		{Command: "CREATEBF", Key: "bloom", Value: "2048", Subkey: "0.001"},
		{Command: "ADDBF", Key: "bloom", Value: "value"},
		{Command: "CREATECMS", Key: "cms", Value: "256", Subkey: "4"},
		{Command: "INCRCMS", Key: "cms", Value: "value", Subkey: "3"},
		{Command: "CREATEHLL", Key: "hll", Value: "10"},
		{Command: "ADDHLL", Key: "hll", Value: "value"},
		{Command: "CREATECF", Key: "cuckoo", Value: "2048", Subkey: "0.001"},
		{Command: "ADDCF", Key: "cuckoo", Value: "value"},
		{Command: "CREATEXF", Key: "xor", Value: "64"},
		{Command: "ADDXF", Key: "xor", Value: "value-a"},
		{Command: "ADDXF", Key: "xor", Value: "value-b"},
		{Command: "BUILDXF", Key: "xor"},
		{Command: "CREATEXF", Key: "xor-staged", Value: "64"},
		{Command: "ADDXF", Key: "xor-staged", Value: "value"},
		{Command: "CREATEFW", Key: "fenwick", Value: "128"},
		{Command: "ADDFW", Key: "fenwick", Value: "32", Subkey: "3"},
		{Command: "CREATEQ", Key: "quantile", Value: "0.01"},
		{Command: "ADDQ", Key: "quantile", Value: "1.5"},
		{Command: "ADDQ", Key: "quantile", Value: "9.5"},
	}
	for _, request := range requests {
		if response := source.ExecuteCommand(request); !response.OK {
			t.Fatalf("%s %s failed: %s", request.Command, request.Key, response.Message)
		}
	}
	roaringValues := make([]uint32, roaringBitmapArrayMaxSize+1)
	sparseValues := make([]uint64, sparseBitsetArrayMaxSize+1)
	for index := range roaringValues {
		roaringValues[index] = uint32(index)
		sparseValues[index] = uint64(index)
	}
	if added := source.AddRoaringBitmap("roaring", roaringValues[0], roaringValues[1:]...); added != len(roaringValues) {
		t.Fatalf("AddRoaringBitmap() = %d, want %d", added, len(roaringValues))
	}
	if added := source.AddSparseBitset("sparse", sparseValues[0], sparseValues[1:]...); added != len(sparseValues) {
		t.Fatalf("AddSparseBitset() = %d, want %d", added, len(sparseValues))
	}
	if !source.Expire("bloom", time.Hour) {
		t.Fatal("Expire(bloom) = false")
	}

	loaded, store := loadColdCommandDumpPersistentTrie(t, source, backend, StorageFormatBinary)
	defer store.Close()
	defer loaded.Destroy()
	for _, key := range []string{"bloom", "cms", "hll", "cuckoo", "xor", "xor-staged", "fenwick", "quantile", "roaring", "sparse"} {
		assertColdLevelDBReference(t, loaded, key)
		assertCommandDumpMatchesSnapshot(t, loaded, key)
	}
}

func BenchmarkCommandDumpColdBytes64KiBReuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendLevelDB, "bytes", func(source *HatTrie) {
		if err := source.UpsertBytesChecked("bytes", bytes.Repeat([]byte{0, 255, 1, 128}, DiskBytesThreshold/4)); err != nil {
			b.Fatalf("UpsertBytesChecked() error = %v", err)
		}
	})
}

func BenchmarkCommandDumpColdPebbleBytes64KiBReuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendPebble, "bytes", func(source *HatTrie) {
		if err := source.UpsertBytesChecked("bytes", bytes.Repeat([]byte{0, 255, 1, 128}, DiskBytesThreshold/4)); err != nil {
			b.Fatalf("UpsertBytesChecked() error = %v", err)
		}
	})
}

func BenchmarkCommandDumpColdMap64Reuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendLevelDB, "map", func(source *HatTrie) {
		value := make(Map, 64)
		for index := 0; index < 64; index++ {
			value[fmt.Sprintf("field-%02d", index)] = fmt.Sprintf("value-%02d", index)
		}
		if err := source.UpsertMapChecked("map", value); err != nil {
			b.Fatalf("UpsertMapChecked() error = %v", err)
		}
	})
}

func BenchmarkCommandDumpColdBloomReuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendLevelDB, "bloom:key", func(source *HatTrie) {
		setupCommandFeatureBloomWithValue(b, source)
	})
}

func BenchmarkCommandDumpColdBuiltXorReuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendLevelDB, "xor", func(source *HatTrie) {
		if err := source.UpsertXorFilter("xor", 4096); err != nil {
			b.Fatalf("UpsertXorFilter() error = %v", err)
		}
		for index := 0; index < 4096; index++ {
			if _, err := source.AddXorFilterChecked("xor", fmt.Sprintf("value-%04d", index)); err != nil {
				b.Fatalf("AddXorFilterChecked() error = %v", err)
			}
		}
		if _, ok, err := source.BuildXorFilter("xor"); err != nil || !ok {
			b.Fatalf("BuildXorFilter() = %v/%v", ok, err)
		}
	})
}

func BenchmarkCommandDumpColdStagedXorControlReuse(b *testing.B) {
	benchmarkCommandDumpColdPersistentReuse(b, StorageBackendLevelDB, "xor", func(source *HatTrie) {
		if err := source.UpsertXorFilter("xor", 64); err != nil {
			b.Fatalf("UpsertXorFilter() error = %v", err)
		}
		for index := 0; index < 64; index++ {
			if _, err := source.AddXorFilterChecked("xor", fmt.Sprintf("value-%02d", index)); err != nil {
				b.Fatalf("AddXorFilterChecked() error = %v", err)
			}
		}
	})
}

func benchmarkCommandDumpColdPersistentReuse(b *testing.B, backend StorageBackend, key string, setup func(*HatTrie)) {
	b.Helper()
	source := CreateHatTrie()
	setup(source)
	path := filepath.Join(b.TempDir(), "cache")
	store, err := OpenPersistentStoreWithFormat(path, backend, StorageFormatBinary)
	if err != nil {
		source.Destroy()
		b.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
	}
	if err := store.Save(source); err != nil {
		store.Close()
		source.Destroy()
		b.Fatalf("Save() error = %v", err)
	}
	source.Destroy()
	defer store.Close()
	loaded := CreateHatTrie()
	defer loaded.Destroy()
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil || result.ValuesLoaded != 0 {
		b.Fatalf("LoadWithPolicy() = %#v/%v, want cold references", result, err)
	}
	assertColdLevelDBReference(b, loaded, key)
	benchmarkCommandDumpReuse(b, loaded, key)
}

func loadColdCommandDumpPersistentTrie(t *testing.T, source *HatTrie, backend StorageBackend, format StorageFormat) (*HatTrie, PersistentStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPersistentStoreWithFormat(path, backend, format)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
	}
	if err := store.Save(source); err != nil {
		store.Close()
		t.Fatalf("Save() error = %v", err)
	}
	loaded := CreateHatTrie()
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		loaded.Destroy()
		store.Close()
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.ValuesLoaded != 0 {
		loaded.Destroy()
		store.Close()
		t.Fatalf("LoadWithPolicy() result = %#v, want cold references", result)
	}
	return loaded, store
}

func assertColdLevelDBReference(t testing.TB, trie *HatTrie, key string) {
	t.Helper()
	trie.mu.Lock()
	hval := trie.peekCachedLocked(key)
	trie.mu.Unlock()
	if !hval.IsLevelDBReference() {
		t.Fatalf("%s type = %d, want cold LevelDB reference", key, hval.Type())
	}
}
