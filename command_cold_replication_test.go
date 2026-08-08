package hatriecache

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandDumpColdBinaryEncodingMatchesSnapshot(t *testing.T) {
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

	loaded, store := loadColdCommandDumpTrie(t, source, StorageFormatBinary)
	defer store.Close()
	defer loaded.Destroy()
	for _, key := range []string{"counter", "string", "bytes", "map"} {
		assertColdLevelDBReference(t, loaded, key)
		assertCommandDumpMatchesSnapshot(t, loaded, key)
	}
}

func TestCommandDumpColdJSONFallsBackToSnapshot(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	if err := source.UpsertMapChecked("map", Map{"nested": Slice{"value", int64(4)}}); err != nil {
		t.Fatalf("UpsertMapChecked() error = %v", err)
	}

	loaded, store := loadColdCommandDumpTrie(t, source, StorageFormatJSON)
	defer store.Close()
	defer loaded.Destroy()
	assertColdLevelDBReference(t, loaded, "map")
	assertCommandDumpMatchesSnapshot(t, loaded, "map")
}

func TestCommandDumpColdFixedBinaryEncodingMatchesSnapshot(t *testing.T) {
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

	loaded, store := loadColdCommandDumpTrie(t, source, StorageFormatBinary)
	defer store.Close()
	defer loaded.Destroy()
	for _, key := range []string{"bloom", "cms", "hll", "cuckoo", "xor", "fenwick", "quantile", "roaring", "sparse"} {
		assertColdLevelDBReference(t, loaded, key)
		assertCommandDumpMatchesSnapshot(t, loaded, key)
	}
}

func BenchmarkCommandDumpColdBytes64KiBReuse(b *testing.B) {
	benchmarkCommandDumpColdReuse(b, "bytes", func(source *HatTrie) {
		if err := source.UpsertBytesChecked("bytes", bytes.Repeat([]byte{0, 255, 1, 128}, DiskBytesThreshold/4)); err != nil {
			b.Fatalf("UpsertBytesChecked() error = %v", err)
		}
	})
}

func BenchmarkCommandDumpColdMap64Reuse(b *testing.B) {
	benchmarkCommandDumpColdReuse(b, "map", func(source *HatTrie) {
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
	benchmarkCommandDumpColdReuse(b, "bloom:key", func(source *HatTrie) {
		setupCommandFeatureBloomWithValue(b, source)
	})
}

func benchmarkCommandDumpColdReuse(b *testing.B, key string, setup func(*HatTrie)) {
	b.Helper()
	source := CreateHatTrie()
	setup(source)
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	if err := source.SaveLevelDB(path); err != nil {
		source.Destroy()
		b.Fatalf("SaveLevelDB() error = %v", err)
	}
	source.Destroy()
	store, err := OpenLevelDBStore(path)
	if err != nil {
		b.Fatalf("OpenLevelDBStore() error = %v", err)
	}
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

func loadColdCommandDumpTrie(t *testing.T, source *HatTrie, format StorageFormat) (*HatTrie, *LevelDBStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	if err := source.SaveLevelDBWithFormat(path, format); err != nil {
		t.Fatalf("SaveLevelDBWithFormat() error = %v", err)
	}
	store, err := OpenLevelDBStoreWithFormat(path, format)
	if err != nil {
		t.Fatalf("OpenLevelDBStoreWithFormat() error = %v", err)
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
