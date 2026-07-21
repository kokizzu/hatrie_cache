package hatriecache

import (
	"fmt"
	"io"
	"net/http"
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

func BenchmarkCommandResponseWireProtobuf(b *testing.B) {
	response := benchmarkCommandWireResponse()
	request, err := http.NewRequest(http.MethodPost, "/api/commands", nil)
	if err != nil {
		b.Fatal(err)
	}
	request.Header.Set("Accept", commandWireContentTypeProtobuf)
	writer := newDiscardCommandWireResponseWriter()
	wireBytes := benchmarkCommandResponseWireBytes(b, writer, request, response)

	b.ReportAllocs()
	b.ReportMetric(float64(wireBytes), "wire_B/op")
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		writer.reset()
		writeCommandResponseWire(writer, request, http.StatusOK, response, CommandWireFormatProtobuf)
		if writer.status != http.StatusOK || writer.bytes != wireBytes {
			b.Fatalf("writeCommandResponseWire status/bytes = %d/%d, want 200/%d", writer.status, writer.bytes, wireBytes)
		}
	}
}

func BenchmarkCommandWireAcceptNegotiation(b *testing.B) {
	accept := "application/json; charset=utf-8; q=0.2, application/x-protobuf;q=0.9, */*;q=0.1"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		format, ok := commandWireFormatFromAccept(accept, CommandWireFormatProtobuf)
		if !ok || format != CommandWireFormatProtobuf {
			b.Fatalf("commandWireFormatFromAccept() = %q/%v, want protobuf/true", format, ok)
		}
	}
}

func BenchmarkCommandWireContentTypeNegotiation(b *testing.B) {
	contentType := "Application/X-Protobuf ; proto=cache"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		format, ok := commandWireFormatFromContentType(contentType)
		if !ok || format != CommandWireFormatProtobuf {
			b.Fatalf("commandWireFormatFromContentType() = %q/%v, want protobuf/true", format, ok)
		}
	}
}

func BenchmarkAcceptEncodingGzipNegotiation(b *testing.B) {
	request, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		b.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "br;q=1, gzip; foo; q=0.8, *;q=0.1")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !requestAcceptsGzip(request) {
			b.Fatal("requestAcceptsGzip() = false, want true")
		}
	}
}

func BenchmarkAddVaryHeaderDeduplicated(b *testing.B) {
	header := http.Header{}
	header.Add("Vary", "Accept, Accept-Encoding, Origin")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		addVaryHeader(header, "Accept-Encoding")
	}
}

func BenchmarkCommandJournalEncodeJSON(b *testing.B) {
	benchmarkCommandJournalEncodeFormat(b, CommandJournalFormatJSON)
}

func BenchmarkCommandJournalEncodeBinary(b *testing.B) {
	benchmarkCommandJournalEncodeFormat(b, CommandJournalFormatBinary)
}

func BenchmarkCommandJournalDecodeJSON(b *testing.B) {
	benchmarkCommandJournalDecodeFormat(b, CommandJournalFormatJSON)
}

func BenchmarkCommandJournalDecodeBinary(b *testing.B) {
	benchmarkCommandJournalDecodeFormat(b, CommandJournalFormatBinary)
}

func BenchmarkCommandJournalEncodeStructuredJSON(b *testing.B) {
	benchmarkCommandJournalEncodeEntryFormat(b, benchmarkCommandJournalStructuredEntry(), CommandJournalFormatJSON)
}

func BenchmarkCommandJournalEncodeStructuredBinary(b *testing.B) {
	benchmarkCommandJournalEncodeEntryFormat(b, benchmarkCommandJournalStructuredEntry(), CommandJournalFormatBinary)
}

func BenchmarkCommandJournalDecodeStructuredJSON(b *testing.B) {
	benchmarkCommandJournalDecodeEntryFormat(b, benchmarkCommandJournalStructuredEntry(), CommandJournalFormatJSON)
}

func BenchmarkCommandJournalDecodeStructuredBinary(b *testing.B) {
	benchmarkCommandJournalDecodeEntryFormat(b, benchmarkCommandJournalStructuredEntry(), CommandJournalFormatBinary)
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
		if closer, ok := body.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkCommandJournalEncodeFormat(b *testing.B, format CommandJournalFormat) {
	benchmarkCommandJournalEncodeEntryFormat(b, benchmarkCommandJournalEntry(), format)
}

func benchmarkCommandJournalEncodeEntryFormat(b *testing.B, entry commandJournalEntry, format CommandJournalFormat) {
	journalBytes := benchmarkCommandJournalBytes(b, entry, format)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(journalBytes), "journal_B/op")
	for i := 0; i < b.N; i++ {
		if _, err := marshalCommandJournalEntry(entry, format); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCommandJournalDecodeFormat(b *testing.B, format CommandJournalFormat) {
	benchmarkCommandJournalDecodeEntryFormat(b, benchmarkCommandJournalEntry(), format)
}

func benchmarkCommandJournalDecodeEntryFormat(b *testing.B, entry commandJournalEntry, format CommandJournalFormat) {
	data, err := marshalCommandJournalEntry(entry, format)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(data)), "journal_B/op")
	for i := 0; i < b.N; i++ {
		if _, err := decodeCommandJournalEntry(data); err != nil {
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

func benchmarkCommandResponseWireBytes(b *testing.B, writer *discardCommandWireResponseWriter, request *http.Request, response CacheCommandResponse) int {
	b.Helper()
	writer.reset()
	writeCommandResponseWire(writer, request, http.StatusOK, response, CommandWireFormatProtobuf)
	if writer.status != http.StatusOK || writer.bytes == 0 {
		b.Fatalf("writeCommandResponseWire status/bytes = %d/%d, want 200/non-empty", writer.status, writer.bytes)
	}
	return writer.bytes
}

func benchmarkCommandJournalBytes(b *testing.B, entry commandJournalEntry, format CommandJournalFormat) int {
	b.Helper()
	data, err := marshalCommandJournalEntry(entry, format)
	if err != nil {
		b.Fatal(err)
	}
	return len(data)
}

func benchmarkCommandWirePayload() CacheCommandRequest {
	entry := `{"key":"session:1","type":"string","string":"` + strings.Repeat("active-user-", 256) + `"}`
	return CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   entry,
	}
}

func benchmarkCommandWireResponse() CacheCommandResponse {
	return CacheCommandResponse{
		OK:      true,
		Message: "batch applied",
		Responses: []CacheCommandResponse{
			{OK: true, Message: "stored string"},
			{OK: true, Message: "ok", Value: strings.Repeat("active-user-", 256)},
			{OK: true, Message: "stored counter"},
			{OK: true, Message: "ok", Value: "42"},
		},
	}
}

func benchmarkCommandJournalEntry() commandJournalEntry {
	return commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 42,
		Request:  benchmarkCommandWirePayload(),
	}
}

func benchmarkCommandJournalStructuredEntry() commandJournalEntry {
	return commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 43,
		Request: CacheCommandRequest{
			Command: "PUTMAP",
			Key:     "profile:structured",
			Values: Slice{
				strings.Repeat("line\nquoted\"value", 8),
				42,
				Map{"route": "/api/cache", "status": "active"},
			},
			Pairs: Map{
				"notes": Slice{
					strings.Repeat("line\nquoted\"value", 8),
					strings.Repeat("tab\tvalue\\path", 8),
				},
				"profile": Map{
					"name": "ivi",
					"age":  32,
					"tags": Slice{"alpha", "beta", "gamma"},
				},
			},
		},
	}
}

func BenchmarkSnapshotFormatJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatJSON)
}

func BenchmarkSnapshotFormatBinary(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatBinary)
}

func BenchmarkSnapshotFormatGzipJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipJSON)
}

func BenchmarkSnapshotFormatGzipBestJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipBestJSON)
}

func BenchmarkSnapshotFormatGzipBinary(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipBinary)
}

func BenchmarkSnapshotFormatGzipBestBinary(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipBestBinary)
}

func BenchmarkSnapshotFormatStructuredJSON(b *testing.B) {
	benchmarkSnapshotFormatStructured(b, SnapshotFormatJSON)
}

func BenchmarkSnapshotFormatStructuredBinary(b *testing.B) {
	benchmarkSnapshotFormatStructured(b, SnapshotFormatBinary)
}

func BenchmarkSnapshotFormatStructuredGzipBestJSON(b *testing.B) {
	benchmarkSnapshotFormatStructured(b, SnapshotFormatGzipBestJSON)
}

func BenchmarkSnapshotFormatStructuredGzipBestBinary(b *testing.B) {
	benchmarkSnapshotFormatStructured(b, SnapshotFormatGzipBestBinary)
}

func BenchmarkLevelDBSaveMaterialized(b *testing.B) {
	benchmarkLevelDBSaveMaterializedFormat(b, DefaultStorageFormat)
}

func BenchmarkLevelDBSaveMaterializedJSON(b *testing.B) {
	benchmarkLevelDBSaveMaterializedFormat(b, StorageFormatJSON)
}

func BenchmarkLevelDBSaveStructuredMaterialized(b *testing.B) {
	benchmarkLevelDBSaveStructuredMaterializedFormat(b, DefaultStorageFormat)
}

func BenchmarkLevelDBSaveStructuredMaterializedJSON(b *testing.B) {
	benchmarkLevelDBSaveStructuredMaterializedFormat(b, StorageFormatJSON)
}

func benchmarkLevelDBSaveMaterializedFormat(b *testing.B, format StorageFormat) {
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, format)
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

func benchmarkLevelDBSaveStructuredMaterializedFormat(b *testing.B, format StorageFormat) {
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, format)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	trie, _ := benchmarkStructuredSnapshotTrie(b)
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

func BenchmarkLevelDBSaveDirtyCompareMode(b *testing.B) {
	tests := []struct {
		name string
		mode LevelDBCompareBeforeWriteMode
		keys int
	}{
		{name: "always-128-changed", mode: LevelDBCompareBeforeWriteAlways, keys: 128},
		{name: "never-128-changed", mode: LevelDBCompareBeforeWriteNever, keys: 128},
		{name: "auto-128-changed", mode: LevelDBCompareBeforeWriteAuto, keys: 128},
		{name: "always-2048-changed", mode: LevelDBCompareBeforeWriteAlways, keys: 2048},
		{name: "never-2048-changed", mode: LevelDBCompareBeforeWriteNever, keys: 2048},
		{name: "auto-2048-changed", mode: LevelDBCompareBeforeWriteAuto, keys: 2048},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkLevelDBSaveDirtyCompareMode(b, tt.mode, tt.keys)
		})
	}
}

func BenchmarkLevelDBDirtyTrackerMarkSnapshotClear(b *testing.B) {
	for _, keys := range []int{8, 32, 128, 4096} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			benchKeys := make([]string, keys)
			for idx := range benchKeys {
				benchKeys[idx] = fmt.Sprintf("dirty:%04d", idx)
			}
			tracker := NewLevelDBDirtyTracker()

			b.ReportAllocs()
			b.ReportMetric(float64(keys), "dirty_keys/op")
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for _, key := range benchKeys {
					tracker.Mark(key)
				}
				snapshot := tracker.Snapshot()
				tracker.Clear(snapshot)
			}
		})
	}
}

func BenchmarkLevelDBLoadMaterialized(b *testing.B) {
	store := benchmarkPopulatedLevelDBStoreWithFormat(b, DefaultStorageFormat)
	benchmarkLevelDBLoad(b, store, LevelDBLoadPolicy{}, benchmarkSnapshotEntries)
}

func BenchmarkLevelDBLoadMaterializedJSON(b *testing.B) {
	store := benchmarkPopulatedLevelDBStoreWithFormat(b, StorageFormatJSON)
	benchmarkLevelDBLoad(b, store, LevelDBLoadPolicy{}, benchmarkSnapshotEntries)
}

func BenchmarkLevelDBLoadStructuredMaterialized(b *testing.B) {
	store, entries := benchmarkPopulatedStructuredLevelDBStoreWithFormat(b, DefaultStorageFormat)
	benchmarkLevelDBLoadEntries(b, store, LevelDBLoadPolicy{}, entries, entries)
}

func BenchmarkLevelDBLoadStructuredMaterializedJSON(b *testing.B) {
	store, entries := benchmarkPopulatedStructuredLevelDBStoreWithFormat(b, StorageFormatJSON)
	benchmarkLevelDBLoadEntries(b, store, LevelDBLoadPolicy{}, entries, entries)
}

func BenchmarkStructuredStorageFallbackEncode(b *testing.B) {
	entries := benchmarkStructuredFallbackEntries()
	recordBytes := 0
	for _, entry := range entries {
		data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
		if err != nil {
			b.Fatal(err)
		}
		recordBytes += len(data)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for i := 0; i < b.N; i++ {
		for _, entry := range entries {
			if _, err := marshalLevelDBEntry(entry, StorageFormatBinary); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkStructuredStorageFallbackDecode(b *testing.B) {
	entries := benchmarkStructuredFallbackEntries()
	records := make([][]byte, len(entries))
	recordBytes := 0
	for idx, entry := range entries {
		data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
		if err != nil {
			b.Fatal(err)
		}
		records[idx] = data
		recordBytes += len(data)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for i := 0; i < b.N; i++ {
		for _, data := range records {
			if _, err := decodeLevelDBEntry(data); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkStructuredFallbackEntries() []snapshotEntry {
	nested := PriorityQueue{{Priority: 9, Value: "nested"}}
	return []snapshotEntry{
		{Key: "small", Type: "map", Map: Map{"a": "b"}},
		{Key: "map", Type: "map", Map: Map{"queue": nested}},
		{Key: "queue", Type: "priority_queue", PriorityQueue: []priorityQueueItem{{Priority: 1, Sequence: 1, Value: nested}}},
		{Key: "top-k", Type: "top_k", TopK: &topKSnapshot{Capacity: 2, Total: 1, Items: []topKItem{{Key: "nested", Value: nested, Count: 1}}}},
		{Key: "radix", Type: "radix_tree", RadixTree: &radixTreeSnapshot{Count: 1, Items: []RadixTreeItem{{Key: "queue", Value: nested}}}},
		{Key: "sample", Type: "reservoir_sample", ReservoirSample: &reservoirSampleSnapshot{Capacity: 1, Seen: 1, Items: []reservoirSampleItem{{Value: nested, Priority: 1, Sequence: 1}}}},
		{Key: "xor", Type: "xor_filter", XorFilter: &xorFilterSnapshot{ExpectedItems: 8, Items: 1, Staged: []xorFilterStagedItem{{Key: `"staged"`, Value: "staged"}}}},
	}
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

func benchmarkSnapshotFormatStructured(b *testing.B, format SnapshotFormat) {
	trie, _ := benchmarkStructuredSnapshotTrie(b)
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
const benchmarkStructuredSnapshotGroups = 16

func benchmarkStructuredSnapshotTrie(tb testing.TB) (*HatTrie, int) {
	tb.Helper()
	trie := CreateHatTrie()
	entries := 0
	for group := 0; group < benchmarkStructuredSnapshotGroups; group++ {
		prefix := fmt.Sprintf("structured:%02d", group)
		trie.UpsertCounter(prefix+":counter", int32(group))
		entries++
		trie.UpsertString(prefix+":string", strings.Repeat("payload-", 8))
		entries++
		trie.UpsertBytes(prefix+":bytes", []byte(strings.Repeat("raw-", 16)))
		entries++
		trie.UpsertMap(prefix+":map", Map{"route": "/api/cache", "status": "active", "count": group})
		entries++
		trie.UpsertSlice(prefix+":slice", Slice{"alpha", group, Map{"nested": true}})
		entries++
		trie.UpsertSet(prefix+":set", Set{"alpha", "beta", group})
		entries++
		trie.UpsertPriorityQueue(prefix+":queue", PriorityQueue{{Priority: int64(group + 1), Value: "alpha"}, {Priority: int64(group + 2), Value: Map{"route": "/api/cache"}}})
		entries++
		benchmarkMust(tb, trie.UpsertBloomFilter(prefix+":bloom", 256, 0.01))
		trie.AddBloomFilter(prefix+":bloom", "alpha", "beta", group)
		entries++
		benchmarkMust(tb, trie.UpsertCountMinSketch(prefix+":cms", 64, 4))
		trie.IncrementCountMinSketch(prefix+":cms", "alpha", 3)
		trie.IncrementCountMinSketch(prefix+":cms", "beta", 5)
		entries++
		benchmarkMust(tb, trie.UpsertHyperLogLog(prefix+":hll", 10))
		trie.AddHyperLogLog(prefix+":hll", "alpha", "beta", group)
		entries++
		benchmarkMust(tb, trie.UpsertTopK(prefix+":topk", 8))
		trie.AddTopK(prefix+":topk", "alpha", 5)
		trie.AddTopK(prefix+":topk", Map{"route": "/api/cache"}, 3)
		entries++
		benchmarkMust(tb, trie.UpsertCuckooFilter(prefix+":cuckoo", 128, 0.01))
		trie.AddCuckooFilter(prefix+":cuckoo", "alpha", "beta", group)
		entries++
		trie.UpsertRoaringBitmap(prefix + ":roaring")
		trie.AddRoaringBitmap(prefix+":roaring", 1, 65543, uint32(group))
		entries++
		trie.UpsertSparseBitset(prefix + ":sparse")
		trie.AddSparseBitset(prefix+":sparse", 1, 65543, ^uint64(0)-uint64(group))
		entries++
		benchmarkMust(tb, trie.UpsertQuantileSketch(prefix+":quantile", 0.01))
		trie.AddQuantileSketch(prefix+":quantile", 10.5, 20.5, 30.5, float64(group))
		entries++
		benchmarkMust(tb, trie.UpsertFenwickTree(prefix+":fenwick", 16))
		trie.AddFenwickTree(prefix+":fenwick", 2, 5)
		trie.AddFenwickTree(prefix+":fenwick", 9, int64(group))
		entries++
		benchmarkMust(tb, trie.UpsertReservoirSample(prefix+":sample", 8))
		trie.AddReservoirSample(prefix+":sample", "alpha", "beta", Map{"route": "/api/cache"}, group)
		entries++
		benchmarkMust(tb, trie.UpsertXorFilter(prefix+":xor", 8))
		if _, err := trie.AddXorFilter(prefix+":xor", "alpha", "beta", group); err != nil {
			trie.Destroy()
			tb.Fatalf("AddXorFilter(%s:xor) error = %v", prefix, err)
		}
		if _, _, err := trie.BuildXorFilter(prefix + ":xor"); err != nil {
			trie.Destroy()
			tb.Fatalf("BuildXorFilter(%s:xor) error = %v", prefix, err)
		}
		entries++
		trie.UpsertRadixTree(prefix + ":radix")
		trie.PutRadixTree(prefix+":radix", "user:100/profile", Map{"active": true, "group": group})
		trie.PutRadixTree(prefix+":radix", "user:101/score", group)
		entries++
	}
	return trie, entries
}

func benchmarkMust(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("benchmark setup error = %v", err)
	}
}

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
	return benchmarkPopulatedLevelDBStoreWithFormat(b, DefaultStorageFormat)
}

func benchmarkPopulatedLevelDBStoreWithFormat(b *testing.B, format StorageFormat) *LevelDBStore {
	b.Helper()
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, format)
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

func benchmarkPopulatedStructuredLevelDBStoreWithFormat(b *testing.B, format StorageFormat) (*LevelDBStore, int) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, format)
	if err != nil {
		b.Fatal(err)
	}
	source, entries := benchmarkStructuredSnapshotTrie(b)
	if err := store.Save(source); err != nil {
		source.Destroy()
		_ = store.Close()
		b.Fatal(err)
	}
	source.Destroy()
	b.Cleanup(func() {
		_ = store.Close()
	})
	return store, entries
}

func benchmarkLevelDBLoad(b *testing.B, store *LevelDBStore, policy LevelDBLoadPolicy, wantValuesLoaded int) {
	benchmarkLevelDBLoadEntries(b, store, policy, benchmarkSnapshotEntries, wantValuesLoaded)
}

func benchmarkLevelDBLoadEntries(b *testing.B, store *LevelDBStore, policy LevelDBLoadPolicy, wantKeysLoaded int, wantValuesLoaded int) {
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
		if result.KeysLoaded != wantKeysLoaded || result.ValuesLoaded != wantValuesLoaded {
			trie.Destroy()
			b.Fatalf("LoadWithPolicy() = %#v, want %d keys and %d values", result, wantKeysLoaded, wantValuesLoaded)
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

func benchmarkLevelDBSaveDirtyCompareMode(b *testing.B, mode LevelDBCompareBeforeWriteMode, keys int) {
	path := filepath.Join(b.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	benchKeys := make([]string, keys)
	for idx := range benchKeys {
		key := fmt.Sprintf("dirty:%04d", idx)
		benchKeys[idx] = key
		trie.UpsertString(key, "initial")
	}
	if err := store.Save(trie); err != nil {
		b.Fatal(err)
	}
	tracker := NewLevelDBDirtyTracker()
	options := LevelDBSaveOptions{CompareBeforeWrite: mode}

	b.ReportAllocs()
	b.ReportMetric(float64(keys), "dirty_keys/op")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		value := fmt.Sprintf("value:%d", iter)
		for _, key := range benchKeys {
			trie.UpsertString(key, value)
			tracker.Mark(key)
		}
		b.StartTimer()
		if err := store.SaveDirtyWithOptions(trie, tracker, options); err != nil {
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
