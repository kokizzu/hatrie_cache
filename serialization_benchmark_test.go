package hatriecache

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkCommandWireJSON(b *testing.B) {
	benchmarkCommandWireFormat(b, CommandWireFormatJSON)
}

func BenchmarkCommandWireProtobuf(b *testing.B) {
	benchmarkCommandWireFormat(b, CommandWireFormatProtobuf)
}

func benchmarkCommandWireFormat(b *testing.B, format CommandWireFormat) {
	payload := benchmarkCommandWirePayload()
	wireBytes := benchmarkCommandWireBytes(b, payload, format)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(wireBytes), "wire_B/op")
	for i := 0; i < b.N; i++ {
		body, _, _, err := commandRequestBody(payload, format, estimatedReplicationRequestBytes(payload), minCompressedReplicationRequestBytes)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCommandWireBytes(b *testing.B, payload CacheCommandRequest, format CommandWireFormat) int {
	b.Helper()
	body, _, _, err := commandRequestBody(payload, format, estimatedReplicationRequestBytes(payload), minCompressedReplicationRequestBytes)
	if err != nil {
		b.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		b.Fatal(err)
	}
	return len(data)
}

func benchmarkCommandWirePayload() CacheCommandRequest {
	entry := `{"key":"session:1","type":"string","string":"` + strings.Repeat("active-user-", 256) + `","map":null,"slice":null,"set":null,"priority_queue":null}`
	return CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   entry,
	}
}

func BenchmarkSnapshotFormatJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatJSON)
}

func BenchmarkSnapshotFormatGzipJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipJSON)
}

func BenchmarkSnapshotFormatGzipBestJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipBestJSON)
}

func BenchmarkLevelDBSaveMaterialized(b *testing.B) {
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	trie := benchmarkSnapshotTrie()
	defer trie.Destroy()
	if err := store.Save(trie); err != nil {
		b.Fatal(err)
	}
	benchmarkLevelDBSave(b, store, trie)
}

func BenchmarkLevelDBSaveColdReferences(b *testing.B) {
	store, trie := benchmarkColdLevelDBTrie(b)
	benchmarkLevelDBSave(b, store, trie)
}

func BenchmarkLevelDBSaveColdReferencesStatsChanged(b *testing.B) {
	store, trie := benchmarkColdLevelDBTrie(b)
	for idx := 0; idx < benchmarkSnapshotEntries; idx++ {
		if !trie.Exists(fmt.Sprintf("session:%04d", idx)) {
			b.Fatalf("Exists(session:%04d) = false, want true", idx)
		}
	}
	benchmarkLevelDBSave(b, store, trie)
}

func BenchmarkLevelDBLoadMaterialized(b *testing.B) {
	store := benchmarkPopulatedLevelDBStore(b)
	benchmarkLevelDBLoad(b, store, LevelDBLoadPolicy{}, benchmarkSnapshotEntries)
}

func BenchmarkLevelDBLoadColdReferences(b *testing.B) {
	store := benchmarkPopulatedLevelDBStore(b)
	benchmarkLevelDBLoad(b, store, DefaultLevelDBHotLoadPolicy(), 0)
}

func benchmarkSnapshotFormat(b *testing.B, format SnapshotFormat) {
	trie := benchmarkSnapshotTrie()
	defer trie.Destroy()
	diskBytes := benchmarkSnapshotBytes(b, trie, format)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(diskBytes), "disk_B/op")
	for i := 0; i < b.N; i++ {
		if err := trie.writeSnapshot(io.Discard, 42, format); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSnapshotBytes(b *testing.B, trie *HatTrie, format SnapshotFormat) int {
	b.Helper()
	var builder strings.Builder
	if err := trie.writeSnapshot(&builder, 42, format); err != nil {
		b.Fatal(err)
	}
	return builder.Len()
}

func benchmarkSnapshotTrie() *HatTrie {
	trie := CreateHatTrie()
	payload := strings.Repeat("payload-", 64)
	for idx := 0; idx < benchmarkSnapshotEntries; idx++ {
		trie.UpsertString(fmt.Sprintf("session:%04d", idx), payload)
	}
	return trie
}

const benchmarkSnapshotEntries = 512

func benchmarkColdLevelDBTrie(b *testing.B) (*LevelDBStore, *HatTrie) {
	b.Helper()
	store := benchmarkPopulatedLevelDBStore(b)
	trie := CreateHatTrie()
	result, err := store.LoadWithPolicy(trie, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		trie.Destroy()
		_ = store.Close()
		b.Fatal(err)
	}
	if result.KeysLoaded != benchmarkSnapshotEntries || result.ValuesLoaded != 0 {
		trie.Destroy()
		_ = store.Close()
		b.Fatalf("LoadWithPolicy() = %#v, want %d cold keys", result, benchmarkSnapshotEntries)
	}
	b.Cleanup(func() {
		trie.Destroy()
		_ = store.Close()
	})
	return store, trie
}

func benchmarkPopulatedLevelDBStore(b *testing.B) *LevelDBStore {
	b.Helper()
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		b.Fatal(err)
	}
	source := benchmarkSnapshotTrie()
	if err := store.Save(source); err != nil {
		source.Destroy()
		_ = store.Close()
		b.Fatal(err)
	}
	source.Destroy()
	b.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func benchmarkLevelDBLoad(b *testing.B, store *LevelDBStore, policy LevelDBLoadPolicy, wantValuesLoaded int) {
	recordBytes := benchmarkLevelDBRecordBytes(b, store)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	b.ReportMetric(float64(wantValuesLoaded), "values_loaded/op")
	for i := 0; i < b.N; i++ {
		trie := CreateHatTrie()
		result, err := store.LoadWithPolicy(trie, policy)
		if err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		if result.KeysLoaded != benchmarkSnapshotEntries || result.ValuesLoaded != wantValuesLoaded {
			trie.Destroy()
			b.Fatalf("LoadWithPolicy() = %#v, want %d keys and %d values", result, benchmarkSnapshotEntries, wantValuesLoaded)
		}
		trie.Destroy()
	}
}

func benchmarkLevelDBSave(b *testing.B, store *LevelDBStore, trie *HatTrie) {
	recordBytes := benchmarkLevelDBRecordBytes(b, store)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for i := 0; i < b.N; i++ {
		if err := store.Save(trie); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkLevelDBRecordBytes(b *testing.B, store *LevelDBStore) int {
	b.Helper()
	db, unlock, err := store.lockDB()
	if err != nil {
		b.Fatal(err)
	}
	defer unlock()
	snapshot, err := db.GetSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Release()

	total := 0
	if err := scanLevelDBSnapshotEntryData(snapshot, func(_ snapshotEntry, data []byte) error {
		total += len(data)
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return total
}
