package hatriecache

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

type blockingSnapshotWriter struct {
	buffer  bytes.Buffer
	entered chan struct{}
	release chan struct{}
	blocked bool
}

func (writer *blockingSnapshotWriter) Write(data []byte) (int, error) {
	if !writer.blocked {
		writer.blocked = true
		close(writer.entered)
		<-writer.release
	}
	return writer.buffer.Write(data)
}

func TestSnapshotOutputDoesNotBlockMutationsAndKeepsCapturedValue(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "before")
	writer := &blockingSnapshotWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	written := make(chan error, 1)
	go func() {
		written <- ht.writeSnapshot(writer, 0, SnapshotFormatBinary)
	}()
	<-writer.entered

	mutated := make(chan struct{})
	go func() {
		ht.UpsertString("key", "after")
		close(mutated)
	}()
	mutationCompletedDuringOutput := false
	select {
	case <-mutated:
		mutationCompletedDuringOutput = true
	case <-time.After(500 * time.Millisecond):
	}
	close(writer.release)
	if err := <-written; err != nil {
		t.Fatalf("writeSnapshot() error = %v", err)
	}
	<-mutated
	if !mutationCompletedDuringOutput {
		t.Fatal("snapshot output held the trie lock while the writer was blocked")
	}

	path := filepath.Join(t.TempDir(), "captured.hc")
	if err := os.WriteFile(path, writer.buffer.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := restored.GetString("key"); got != "before" {
		t.Fatalf("captured GetString(key) = %q, want before", got)
	}
	if got := ht.GetString("key"); got != "after" {
		t.Fatalf("live GetString(key) = %q, want after", got)
	}
}

func TestLevelDBFullSaveRecordOutputDoesNotBlockMutations(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "before")
	entered := make(chan struct{})
	release := make(chan struct{})
	written := make(chan error, 1)
	var captured []byte
	go func() {
		written <- ht.scanLevelDBEntryDataForStore(nil, nil, StorageFormatBinary, func(key string, data []byte) error {
			captured = cloneBytes(data)
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	mutated := make(chan struct{})
	go func() {
		ht.UpsertString("key", "after")
		close(mutated)
	}()
	mutationCompletedDuringOutput := false
	select {
	case <-mutated:
		mutationCompletedDuringOutput = true
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-written; err != nil {
		t.Fatalf("scanLevelDBEntryDataForStore() error = %v", err)
	}
	<-mutated
	if !mutationCompletedDuringOutput {
		t.Fatal("LevelDB full-save output held the trie lock while the visitor was blocked")
	}
	entry, err := decodeLevelDBEntryForKey("key", captured)
	if err != nil {
		t.Fatalf("decodeLevelDBEntryForKey() error = %v", err)
	}
	if entry.String != "before" {
		t.Fatalf("captured LevelDB string = %q, want before", entry.String)
	}
}

func TestSnapshotCaptureUsesBoundedPages(t *testing.T) {
	ht := newTestTrie(t)
	for idx := 0; idx < snapshotCapturePageEntries+1; idx++ {
		ht.UpsertCounter(fmt.Sprintf("key:%05d", idx), int32(idx))
	}
	capture, err := ht.captureSnapshot()
	if err != nil {
		t.Fatalf("captureSnapshot() error = %v", err)
	}
	if capture.count != snapshotCapturePageEntries+1 {
		t.Fatalf("capture count = %d, want %d", capture.count, snapshotCapturePageEntries+1)
	}
	if len(capture.pages) != 2 {
		t.Fatalf("capture pages = %d, want 2", len(capture.pages))
	}
	if len(capture.pages[0]) != snapshotCapturePageEntries || len(capture.pages[1]) != 1 {
		t.Fatalf("capture page lengths = %d/%d, want %d/1", len(capture.pages[0]), len(capture.pages[1]), snapshotCapturePageEntries)
	}
	for idx, page := range capture.pages {
		if cap(page) > snapshotCapturePageEntries {
			t.Fatalf("capture page %d capacity = %d, want <= %d", idx, cap(page), snapshotCapturePageEntries)
		}
	}
}

func TestSnapshotRoundTripRestoresValuesAndTTL(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertCounter("counter", 42)
	ht.UpsertString("string", "value")
	ht.UpsertBytes("bytes", []byte("payload"))
	ht.UpsertMap("map", Map{"name": "ivi", "age": json.Number("32")})
	ht.UpsertSlice("slice", Slice{"a", json.Number("2")})
	ht.UpsertSet("set", Set{"a", json.Number("2"), "a"})
	ht.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 5, Value: json.Number("2")}, {Priority: 1, Value: "urgent"}})
	if err := ht.UpsertBloomFilter("bloom", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	ht.AddBloomFilter("bloom", "alpha", "beta")
	if err := ht.UpsertCountMinSketch("freq", 128, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	ht.IncrementCountMinSketch("freq", "alpha", 5)
	if err := ht.UpsertHyperLogLog("card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	ht.AddHyperLogLog("card", "alpha", "beta")
	if err := ht.UpsertTopK("top", 3); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	ht.AddTopK("top", "alpha", 5)
	if err := ht.UpsertQuantileSketch("latency", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	ht.AddQuantileSketch("latency", 10, 20, 30)
	if err := ht.UpsertFenwickTree("scores", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	ht.AddFenwickTree("scores", 2, 5)
	ht.AddFenwickTree("scores", 6, 7)
	if err := ht.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	ht.AddCuckooFilter("cuckoo", "alpha", "beta")
	if err := ht.UpsertXorFilter("xor", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	if _, err := ht.AddXorFilter("xor", "alpha", "beta"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := ht.BuildXorFilter("xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %v/%v, want ok", err, ok)
	}
	ht.UpsertRadixTree("radix")
	ht.PutRadixTree("radix", "user:100/profile", Map{"status": "active"})
	ht.PutRadixTree("radix", "user:101/profile", json.Number("42"))
	ht.UpsertRoaringBitmap("bitmap")
	ht.AddRoaringBitmap("bitmap", 1, 1<<16+7)
	ht.UpsertSparseBitset("bitset")
	ht.AddSparseBitset("bitset", 1, 1<<32+7, ^uint64(0))
	if err := ht.UpsertReservoirSample("sample", 3); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	ht.AddReservoirSample("sample", "alpha", "beta", "gamma", "delta")
	if !ht.Expire("string", time.Minute) {
		t.Fatal("Expire(string) = false, want true")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if got := loaded.GetCounter("counter"); got != 42 {
		t.Fatalf("counter = %d, want 42", got)
	}
	if got := loaded.GetString("string"); got != "value" {
		t.Fatalf("string = %q, want value", got)
	}
	if got := loaded.GetBytes("bytes"); !bytes.Equal(got, []byte("payload")) {
		t.Fatalf("bytes = %q, want payload", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"name": "ivi", "age": json.Number("32")}) {
		t.Fatalf("map = %#v, want restored map", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"a", json.Number("2")}) {
		t.Fatalf("slice = %#v, want restored slice", got)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"a", json.Number("2")}) {
		t.Fatalf("set = %#v, want restored set", got)
	}
	if got := loaded.GetPriorityQueue("priority"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 1, Value: "urgent"}, {Priority: 5, Value: json.Number("2")}}) {
		t.Fatalf("priority queue = %#v, want restored priority order", got)
	}
	if !loaded.HasBloomFilter("bloom", "alpha") || !loaded.HasBloomFilter("bloom", "beta") {
		t.Fatal("loaded Bloom filter does not contain inserted values")
	}
	if info, ok := loaded.BloomFilterInfo("bloom"); !ok || info.Insertions != 2 {
		t.Fatalf("loaded BloomFilterInfo = %#v/%v, want 2 insertions", info, ok)
	}
	if got, ok := loaded.EstimateCountMinSketch("freq", "alpha"); !ok || got != 5 {
		t.Fatalf("loaded Count-Min Sketch estimate = %d/%v, want 5", got, ok)
	}
	if info, ok := loaded.CountMinSketchInfo("freq"); !ok || info.Width != 128 || info.Depth != 4 || info.TotalCount != 5 {
		t.Fatalf("loaded CountMinSketchInfo = %#v/%v, want restored sketch", info, ok)
	}
	if got, ok := loaded.CountHyperLogLog("card"); !ok || got < 2 {
		t.Fatalf("loaded HyperLogLog estimate = %d/%v, want at least 2", got, ok)
	}
	if info, ok := loaded.HyperLogLogInfo("card"); !ok || info.Precision != 10 || info.Observations != 2 {
		t.Fatalf("loaded HyperLogLogInfo = %#v/%v, want restored HyperLogLog", info, ok)
	}
	if got := loaded.EstimateTopK("top", "alpha"); !got.Tracked || got.Count != 5 {
		t.Fatalf("loaded Top-K estimate = %#v, want alpha count 5", got)
	}
	if info, ok := loaded.TopKInfo("top"); !ok || info.Capacity != 3 || info.Total != 5 {
		t.Fatalf("loaded TopKInfo = %#v/%v, want restored Top-K", info, ok)
	}
	if got, ok := loaded.EstimateQuantileSketch("latency", 0.5); !ok || got.Count != 3 || got.Value < 10 || got.Value > 30 {
		t.Fatalf("loaded quantile estimate = %#v/%v, want restored sketch", got, ok)
	}
	if info, ok := loaded.QuantileSketchInfo("latency"); !ok || info.Epsilon != 0.01 || info.Count != 3 {
		t.Fatalf("loaded QuantileSketchInfo = %#v/%v, want restored quantile sketch", info, ok)
	}
	if got, ok := loaded.PrefixSumFenwickTree("scores", 6); !ok || got != 12 {
		t.Fatalf("loaded Fenwick prefix sum = %d/%v, want 12", got, ok)
	}
	if got, ok := loaded.RangeSumFenwickTree("scores", 3, 6); !ok || got != 7 {
		t.Fatalf("loaded Fenwick range sum = %d/%v, want 7", got, ok)
	}
	if info, ok := loaded.FenwickTreeInfo("scores"); !ok || info.Size != 8 || info.Updates != 2 || info.Total != 12 {
		t.Fatalf("loaded FenwickTreeInfo = %#v/%v, want restored Fenwick tree", info, ok)
	}
	if !loaded.HasCuckooFilter("cuckoo", "alpha") || !loaded.HasCuckooFilter("cuckoo", "beta") {
		t.Fatal("loaded Cuckoo filter does not contain inserted values")
	}
	if info, ok := loaded.CuckooFilterInfo("cuckoo"); !ok || info.Count != 2 || info.Capacity < 128 {
		t.Fatalf("loaded CuckooFilterInfo = %#v/%v, want restored Cuckoo filter", info, ok)
	}
	if hit, queryable := loaded.HasXorFilter("xor", "alpha"); !queryable || !hit {
		t.Fatalf("loaded XOR filter alpha = %v/%v, want hit", hit, queryable)
	}
	if info, ok := loaded.XorFilterInfo("xor"); !ok || !info.Built || info.Items != 2 {
		t.Fatalf("loaded XorFilterInfo = %#v/%v, want restored XOR filter", info, ok)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:100/profile"); !ok || !reflect.DeepEqual(value, Map{"status": "active"}) {
		t.Fatalf("loaded radix user:100/profile = %#v/%v, want restored nested value", value, ok)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:101/profile"); !ok || value != json.Number("42") {
		t.Fatalf("loaded radix user:101/profile = %#v/%v, want restored json.Number", value, ok)
	}
	if info, ok := loaded.RadixTreeInfo("radix"); !ok || info.Items != 2 || info.Nodes == 0 {
		t.Fatalf("loaded RadixTreeInfo = %#v/%v, want restored radix tree", info, ok)
	}
	if got := loaded.GetRoaringBitmap("bitmap"); !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7}) {
		t.Fatalf("loaded Roaring bitmap = %#v, want restored integer set", got)
	}
	if info, ok := loaded.RoaringBitmapInfo("bitmap"); !ok || info.Cardinality != 2 || info.Containers != 2 {
		t.Fatalf("loaded RoaringBitmapInfo = %#v/%v, want restored Roaring bitmap", info, ok)
	}
	if got := loaded.GetSparseBitset("bitset"); !reflect.DeepEqual(got, []uint64{1, 1<<32 + 7, ^uint64(0)}) {
		t.Fatalf("loaded sparse bitset = %#v, want restored uint64 set", got)
	}
	if info, ok := loaded.SparseBitsetInfo("bitset"); !ok || info.Cardinality != 3 || info.Containers != 3 {
		t.Fatalf("loaded SparseBitsetInfo = %#v/%v, want restored sparse bitset", info, ok)
	}
	if got := loaded.GetReservoirSample("sample"); len(got) != 3 {
		t.Fatalf("loaded reservoir sample len = %d, want bounded sample capacity 3: %#v", len(got), got)
	}
	if info, ok := loaded.ReservoirSampleInfo("sample"); !ok || info.Capacity != 3 || info.Tracked != 3 || info.Seen != 4 {
		t.Fatalf("loaded ReservoirSampleInfo = %#v/%v, want restored reservoir sample", info, ok)
	}
	if got := loaded.TTL("string"); got != time.Minute {
		t.Fatalf("TTL(string) = %s, want 1m", got)
	}
}

func TestSnapshotPersistenceRejectsNilTrie(t *testing.T) {
	var ht *HatTrie
	savePath := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(savePath); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("SaveSnapshot(nil receiver) error = %v, want ErrNilHatTrie", err)
	}
	if _, err := os.Stat(savePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SaveSnapshot(nil receiver) created path/stat error = %v, want not exist", err)
	}
	loadPath := filepath.Join(t.TempDir(), "missing.json")
	if err := ht.LoadSnapshot(loadPath); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("LoadSnapshot(nil receiver) error = %v, want ErrNilHatTrie", err)
	}
	if _, err := ht.LoadSnapshotWithMetadata(loadPath); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("LoadSnapshotWithMetadata(nil receiver) error = %v, want ErrNilHatTrie", err)
	}
}

func TestParseSnapshotFormat(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  SnapshotFormat
	}{
		{value: "", want: SnapshotFormatGzipBestBinary},
		{value: "binary", want: SnapshotFormatBinary},
		{value: "bin", want: SnapshotFormatBinary},
		{value: "gzip-binary", want: SnapshotFormatGzipBinary},
		{value: "gzip-bin", want: SnapshotFormatGzipBinary},
		{value: "gzip-best-binary", want: SnapshotFormatGzipBestBinary},
		{value: "gzip-small-binary", want: SnapshotFormatGzipBestBinary},
		{value: " gzip-best-json ", want: SnapshotFormatGzipBestJSON},
		{value: "gzip-small-json", want: SnapshotFormatGzipBestJSON},
		{value: "gzip-json", want: SnapshotFormatGzipJSON},
		{value: "gzip", want: SnapshotFormatGzipJSON},
		{value: "json", want: SnapshotFormatJSON},
	} {
		got, err := ParseSnapshotFormat(tt.value)
		if err != nil {
			t.Fatalf("ParseSnapshotFormat(%q) error = %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("ParseSnapshotFormat(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}
	if _, err := ParseSnapshotFormat("msgpack"); err == nil {
		t.Fatal("ParseSnapshotFormat(msgpack) error = nil, want unsupported format error")
	}
}

func TestSnapshotBinaryFormatsWriteBinaryPayloadAndLoad(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "value")
	ht.UpsertBytes("blob", []byte{0, 1, 2, 3})

	for _, tt := range []struct {
		format SnapshotFormat
		gzip   bool
	}{
		{format: SnapshotFormatBinary},
		{format: SnapshotFormatGzipBinary, gzip: true},
		{format: SnapshotFormatGzipBestBinary, gzip: true},
	} {
		t.Run(string(tt.format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.bin")
			if err := ht.SaveSnapshotWithJournalSequenceAndFormat(path, 77, tt.format); err != nil {
				t.Fatalf("SaveSnapshotWithJournalSequenceAndFormat(%s) error = %v", tt.format, err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(%s) error = %v", tt.format, err)
			}
			payload := data
			if tt.gzip {
				if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
					t.Fatalf("%s header = % x, want gzip", tt.format, data[:shortTestHeaderLen(data, 2)])
				}
				reader, err := gzip.NewReader(bytes.NewReader(data))
				if err != nil {
					t.Fatalf("NewReader(%s) error = %v", tt.format, err)
				}
				payload, err = io.ReadAll(reader)
				closeErr := reader.Close()
				if err != nil {
					t.Fatalf("ReadAll(%s gzip) error = %v", tt.format, err)
				}
				if closeErr != nil {
					t.Fatalf("Close(%s gzip) error = %v", tt.format, closeErr)
				}
			}
			if !bytes.HasPrefix(payload, snapshotBinaryMagic) {
				t.Fatalf("%s payload header = % x, want binary snapshot magic", tt.format, payload[:shortTestHeaderLen(payload, len(snapshotBinaryMagic))])
			}

			loaded := newTestTrie(t)
			metadata, err := loaded.LoadSnapshotWithMetadata(path)
			if err != nil {
				t.Fatalf("LoadSnapshotWithMetadata(%s) error = %v", tt.format, err)
			}
			if metadata.JournalSequence != 77 {
				t.Fatalf("%s journal sequence = %d, want 77", tt.format, metadata.JournalSequence)
			}
			if got := loaded.GetString("key"); got != "value" {
				t.Fatalf("%s loaded key = %q, want value", tt.format, got)
			}
			if got := loaded.GetBytes("blob"); !bytes.Equal(got, []byte{0, 1, 2, 3}) {
				t.Fatalf("%s loaded blob = %v, want [0 1 2 3]", tt.format, got)
			}
		})
	}
}

func shortTestHeaderLen(data []byte, limit int) int {
	if len(data) < limit {
		return len(data)
	}
	return limit
}

func TestSnapshotBinaryReaderRejectsTruncatedRecord(t *testing.T) {
	writer := newBinaryFieldWriter(snapshotBinaryMagic, len(snapshotBinaryMagic)+(2*binaryFieldMaxVarintLen64))
	writer.writeUvarint(uint64(snapshotVersion))
	writer.writeUvarint(0)
	writer.writeUvarint(32)
	data := append([]byte(nil), writer.bytes()...)
	data = append(data, 0)

	_, err := scanSnapshotFileReader(bytes.NewReader(data), nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("scanSnapshotFileReader(truncated binary) error = %v, want unexpected EOF", err)
	}
}

func TestSnapshotBinaryReaderPreservesEntriesWhenReusingRecordBuffer(t *testing.T) {
	var snapshot bytes.Buffer
	header := newBinaryFieldWriter(snapshotBinaryMagic, len(snapshotBinaryMagic)+(2*binaryFieldMaxVarintLen64))
	header.writeUvarint(uint64(snapshotVersion))
	header.writeUvarint(0)
	if _, err := snapshot.Write(header.bytes()); err != nil {
		t.Fatalf("Write(header) error = %v", err)
	}

	first, err := marshalLevelDBEntry(snapshotEntry{Key: "a", Type: "string", String: strings.Repeat("x", 128)}, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(first) error = %v", err)
	}
	if err := writeSnapshotBinaryRecord(&snapshot, first); err != nil {
		t.Fatalf("writeSnapshotBinaryRecord(first) error = %v", err)
	}
	second, err := marshalLevelDBEntry(snapshotEntry{Key: "b", Type: "string", String: "y"}, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(second) error = %v", err)
	}
	if err := writeSnapshotBinaryRecord(&snapshot, second); err != nil {
		t.Fatalf("writeSnapshotBinaryRecord(second) error = %v", err)
	}

	var entries []snapshotEntry
	_, err = scanSnapshotFileReader(bytes.NewReader(snapshot.Bytes()), func(entry snapshotEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("scanSnapshotFileReader(binary) error = %v", err)
	}
	if len(entries) != 2 || entries[0].Key != "a" || entries[0].String != strings.Repeat("x", 128) || entries[1].Key != "b" || entries[1].String != "y" {
		t.Fatalf("binary snapshot entries = %#v, want preserved decoded entries", entries)
	}
}

func TestSnapshotBinaryRecordBufferDoesNotRetainOversizedRecord(t *testing.T) {
	small, retained := snapshotBinaryRecordBuffer(nil, 32)
	if len(small) != 32 {
		t.Fatalf("small record buffer length = %d, want 32", len(small))
	}
	if cap(retained) != cap(small) || cap(retained) == 0 {
		t.Fatalf("retained small buffer cap = %d, small cap = %d, want retained reusable buffer", cap(retained), cap(small))
	}

	large, nextRetained := snapshotBinaryRecordBuffer(retained, maxSnapshotBinaryReusableRecordBufferBytes+1)
	if len(large) != maxSnapshotBinaryReusableRecordBufferBytes+1 {
		t.Fatalf("large record buffer length = %d, want %d", len(large), maxSnapshotBinaryReusableRecordBufferBytes+1)
	}
	if cap(nextRetained) != cap(retained) {
		t.Fatalf("retained buffer cap after large record = %d, want previous cap %d", cap(nextRetained), cap(retained))
	}

	oversizedRetained := make([]byte, 8, maxSnapshotBinaryReusableRecordBufferBytes+1)
	shrunk, retained := snapshotBinaryRecordBuffer(oversizedRetained, 16)
	if len(shrunk) != 16 {
		t.Fatalf("shrunk record buffer length = %d, want 16", len(shrunk))
	}
	if cap(retained) > maxSnapshotBinaryReusableRecordBufferBytes {
		t.Fatalf("retained buffer cap = %d, want at most %d", cap(retained), maxSnapshotBinaryReusableRecordBufferBytes)
	}
}

func TestSnapshotBinaryReaderRejectsOversizedRecordBeforeAllocation(t *testing.T) {
	writer := newBinaryFieldWriter(snapshotBinaryMagic, len(snapshotBinaryMagic)+(3*binaryFieldMaxVarintLen64))
	writer.writeUvarint(uint64(snapshotVersion))
	writer.writeUvarint(0)
	writer.writeUvarint(maxSnapshotBinaryRecordBytes + 1)

	_, err := scanSnapshotFileReader(bytes.NewReader(writer.bytes()), nil)
	if !errors.Is(err, errSnapshotBinaryRecordTooLarge) {
		t.Fatalf("scanSnapshotFileReader(oversized binary) error = %v, want record too large", err)
	}
}

func TestSnapshotFormatDefaultsToGzipBestBinaryAndLoadsOlderFormats(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("key", "value")

	dir := t.TempDir()
	defaultPath := filepath.Join(dir, "snapshot-default.json")
	if err := ht.SaveSnapshot(defaultPath); err != nil {
		t.Fatalf("SaveSnapshot(default) error = %v", err)
	}
	defaultData, err := os.ReadFile(defaultPath)
	if err != nil {
		t.Fatalf("ReadFile(default) error = %v", err)
	}
	if len(defaultData) < 2 || defaultData[0] != 0x1f || defaultData[1] != 0x8b {
		header := defaultData
		if len(header) > 2 {
			header = header[:2]
		}
		t.Fatalf("default snapshot header = % x, want gzip header", header)
	}
	defaultReader, err := gzip.NewReader(bytes.NewReader(defaultData))
	if err != nil {
		t.Fatalf("NewReader(default snapshot) error = %v", err)
	}
	defaultPayload, err := io.ReadAll(defaultReader)
	defaultCloseErr := defaultReader.Close()
	if err != nil {
		t.Fatalf("ReadAll(default gzip snapshot) error = %v", err)
	}
	if defaultCloseErr != nil {
		t.Fatalf("Close(default gzip snapshot) error = %v", defaultCloseErr)
	}
	if !bytes.HasPrefix(defaultPayload, snapshotBinaryMagic) {
		t.Fatalf("default snapshot payload header = % x, want binary snapshot magic", defaultPayload[:shortTestHeaderLen(defaultPayload, len(snapshotBinaryMagic))])
	}

	gzipBestJSONPath := filepath.Join(dir, "snapshot-best-gzip-json.json")
	if err := ht.SaveSnapshotWithFormat(gzipBestJSONPath, SnapshotFormatGzipBestJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat(gzip-best-json) error = %v", err)
	}

	gzipPath := filepath.Join(dir, "snapshot-fast-gzip.json")
	if err := ht.SaveSnapshotWithFormat(gzipPath, SnapshotFormatGzipJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat(gzip-json) error = %v", err)
	}
	gzipData, err := os.ReadFile(gzipPath)
	if err != nil {
		t.Fatalf("ReadFile(gzip-json) error = %v", err)
	}
	if len(gzipData) < 2 || gzipData[0] != 0x1f || gzipData[1] != 0x8b {
		header := gzipData
		if len(header) > 2 {
			header = header[:2]
		}
		t.Fatalf("gzip-json snapshot header = % x, want gzip header", header)
	}

	jsonPath := filepath.Join(dir, "snapshot-plain.json")
	if err := ht.SaveSnapshotWithFormat(jsonPath, SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat(json) error = %v", err)
	}
	jsonData, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("ReadFile(json) error = %v", err)
	}
	if len(jsonData) == 0 || jsonData[0] != '{' {
		first := jsonData
		if len(first) > 1 {
			first = first[:1]
		}
		t.Fatalf("json snapshot first byte = %q, want object", first)
	}

	for _, path := range []string{defaultPath, gzipBestJSONPath, gzipPath, jsonPath} {
		loaded := newTestTrie(t)
		if err := loaded.LoadSnapshot(path); err != nil {
			t.Fatalf("LoadSnapshot(%s) error = %v", filepath.Base(path), err)
		}
		if got := loaded.GetString("key"); got != "value" {
			t.Fatalf("LoadSnapshot(%s) key = %q, want value", filepath.Base(path), got)
		}
	}
}

func TestSnapshotSavePreservesUnchangedColdLevelDBReferenceRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte(`{"key":"cold","type":"string","string":"value"}`)
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 1 key and 0 values", result)
	}

	var snapshot bytes.Buffer
	if err := loaded.writeSnapshot(&snapshot, 0, SnapshotFormatJSON); err != nil {
		t.Fatalf("writeSnapshot(cold ref) error = %v", err)
	}
	if got := snapshot.String(); !strings.Contains(got, "    "+string(raw)) {
		t.Fatalf("snapshot did not preserve raw cold record:\n%s", got)
	}

	roundTrip := newTestTrie(t)
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(snapshotPath, snapshot.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	if err := roundTrip.LoadSnapshot(snapshotPath); err != nil {
		t.Fatalf("LoadSnapshot(raw cold ref snapshot) error = %v", err)
	}
	if got := roundTrip.GetString("cold"); got != "value" {
		t.Fatalf("round-trip cold value = %q, want value", got)
	}
}

func TestSnapshotSaveConvertsBinaryColdLevelDBReferenceToJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	source := newTestTrie(t)
	source.UpsertString("cold", "value")
	if err := store.Save(source); err != nil {
		t.Fatalf("Save(binary store) error = %v", err)
	}
	data, ok, err := store.entryData("cold")
	if err != nil || !ok {
		t.Fatalf("entryData(cold) = %v/%v, want binary record", err, ok)
	}
	if !levelDBEntryDataIsBinary(data) {
		t.Fatalf("stored cold record = %q, want binary LevelDB record", data)
	}

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 1 cold key and 0 values", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || entries[0].Key != "cold" || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want cold leveldb reference", entries)
	}

	var snapshot bytes.Buffer
	if err := loaded.writeSnapshot(&snapshot, 0, SnapshotFormatJSON); err != nil {
		t.Fatalf("writeSnapshot(binary cold ref) error = %v", err)
	}
	if bytes.Contains(snapshot.Bytes(), levelDBBinaryMagic) {
		t.Fatalf("snapshot leaked binary LevelDB record bytes:\n%s", snapshot.String())
	}
	if got := snapshot.String(); !strings.Contains(got, `"key": "cold"`) || !strings.Contains(got, `"string": "value"`) {
		t.Fatalf("snapshot did not write portable JSON entry:\n%s", got)
	}

	roundTrip := newTestTrie(t)
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(snapshotPath, snapshot.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	if err := roundTrip.LoadSnapshot(snapshotPath); err != nil {
		t.Fatalf("LoadSnapshot(binary cold ref snapshot) error = %v", err)
	}
	if got := roundTrip.GetString("cold"); got != "value" {
		t.Fatalf("round-trip cold value = %q, want value", got)
	}
}

func TestSnapshotSaveRewritesColdLevelDBReferenceWhenStatsChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte(`{"key":"cold","type":"string","string":"value"}`)
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	now := time.Unix(4900, 0)
	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if !loaded.Exists("cold") {
		t.Fatal("Exists(cold) = false, want true")
	}

	var snapshot bytes.Buffer
	if err := loaded.writeSnapshot(&snapshot, 0, SnapshotFormatJSON); err != nil {
		t.Fatalf("writeSnapshot(stats-changed cold ref) error = %v", err)
	}
	if got := snapshot.String(); strings.Contains(got, "    "+string(raw)) {
		t.Fatalf("stats-changed cold ref reused raw record:\n%s", got)
	}

	var entry snapshotEntry
	metadata, err := scanSnapshotFileReader(bytes.NewReader(snapshot.Bytes()), func(candidate snapshotEntry) error {
		entry = candidate
		return nil
	})
	if err != nil {
		t.Fatalf("scanSnapshotFileReader(stats snapshot) error = %v", err)
	}
	if metadata.Version != snapshotVersion {
		t.Fatalf("snapshot version = %d, want %d", metadata.Version, snapshotVersion)
	}
	if entry.Key != "cold" || entry.String != "value" {
		t.Fatalf("snapshot entry = %#v, want cold string value", entry)
	}
	if entry.Stats == nil || entry.Stats.Reads != 1 || entry.Stats.Hits != 1 || entry.Stats.LastHit != now {
		t.Fatalf("snapshot stats = %#v, want one hit at %s", entry.Stats, now)
	}
}

func TestSnapshotRoundTripPreservesBlankKeys(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "empty")
	ht.UpsertString(" ", "space")
	ht.UpsertString("\t", "tab")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	for key, want := range map[string]string{
		"":   "empty",
		" ":  "space",
		"\t": "tab",
	} {
		if got := loaded.GetString(key); got != want {
			t.Fatalf("GetString(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestSaveSnapshotStreamsLargeHistory(t *testing.T) {
	ht := newTestTrie(t)
	large := strings.Repeat("x", 70*1024)
	for idx := 0; idx < 64; idx++ {
		ht.UpsertString(fmt.Sprintf("key:%03d", idx), large)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshotWithJournalSequence(path, 123); err != nil {
		t.Fatalf("SaveSnapshotWithJournalSequence() error = %v", err)
	}

	loaded := newTestTrie(t)
	metadata, err := loaded.LoadSnapshotWithMetadata(path)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 123 {
		t.Fatalf("journal sequence = %d, want 123", metadata.JournalSequence)
	}
	if got := loaded.Size(); got != 64 {
		t.Fatalf("loaded snapshot size = %d, want 64", got)
	}
	if got := loaded.GetString("key:063"); got != large {
		t.Fatalf("loaded large value length = %d, want %d", len(got), len(large))
	}
}

func TestPriorityQueueSnapshotPreservesTieOrderAndNextSequence(t *testing.T) {
	ht := newTestTrie(t)
	if added := ht.PushPriorityQueue("jobs", 1, "first", "second"); added != 2 {
		t.Fatalf("PushPriorityQueue(first, second) = %d, want 2", added)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if added := loaded.PushPriorityQueue("jobs", 1, "third"); added != 1 {
		t.Fatalf("PushPriorityQueue(third) = %d, want 1", added)
	}

	for _, want := range []string{"first", "second", "third"} {
		got, ok := loaded.PopPriorityQueue("jobs")
		if !ok || got.Priority != 1 || got.Value != want {
			t.Fatalf("PopPriorityQueue() = %#v/%v, want %q priority 1", got, ok, want)
		}
	}
}

func TestWriteFileAtomicReplacesFileAndCleansTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")

	if err := writeFileAtomic(path, []byte("first")); err != nil {
		t.Fatalf("writeFileAtomic(first) error = %v", err)
	}
	if err := writeFileAtomic(path, []byte("second")); err != nil {
		t.Fatalf("writeFileAtomic(second) error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("file payload = %q, want second", data)
	}
	assertNoAtomicTempFiles(t, dir, "data.json")
}

func TestWriteFileAtomicStreamCleansTemporaryFileOnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	errBoom := errors.New("boom")

	err := writeFileAtomicStream(path, func(writer io.Writer) error {
		if _, err := writer.Write([]byte("partial")); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("writeFileAtomicStream() error = %v, want boom", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("target Stat() error = %v, want not exist", err)
	}
	assertNoAtomicTempFiles(t, dir, "data.json")
}

func TestWriteJSONFileAtomicStreamsAndCleansTemporaryFileOnEncodeError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")

	if err := writeJSONFileAtomic(path, map[string]interface{}{"sequence": 1}); err != nil {
		t.Fatalf("writeJSONFileAtomic(previous) error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	if err := writeJSONFileAtomic(path, map[string]interface{}{"bad": make(chan int)}); err == nil {
		t.Fatal("writeJSONFileAtomic(unsupported value) error = nil, want error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("file after failed JSON write = %q, want previous %q", after, before)
	}
	assertNoAtomicTempFiles(t, dir, "data.json")
}

func TestWriteSnapshotEntryFieldsJSONOmitsUnrelatedNullFields(t *testing.T) {
	value := snapshotEntry{
		Key:    "profile",
		Type:   "string",
		String: "ivi",
	}

	var buf bytes.Buffer
	if err := writeSnapshotEntryFieldsJSON(&buf, value, "    "); err != nil {
		t.Fatalf("writeSnapshotEntryFieldsJSON() error = %v", err)
	}
	want := "    {\n      \"key\": \"profile\",\n      \"type\": \"string\",\n      \"string\": \"ivi\"\n    }"
	if got := buf.String(); got != want {
		t.Fatalf("writeSnapshotEntryFieldsJSON() = %q, want %q", got, want)
	}
	for _, field := range []string{"map", "slice", "set", "priority_queue"} {
		if strings.Contains(buf.String(), field) {
			t.Fatalf("writeSnapshotEntryFieldsJSON() = %q, want no unrelated %s field", buf.String(), field)
		}
	}
	if strings.HasSuffix(buf.String(), "\n") {
		t.Fatalf("writeSnapshotEntryFieldsJSON() added trailing newline: %q", buf.String())
	}
}

func TestMarshalSnapshotEntryJSONOmitsUnrelatedNullFields(t *testing.T) {
	data, err := marshalSnapshotEntryJSON(snapshotEntry{
		Key:    "profile",
		Type:   "string",
		String: "ivi",
	})
	if err != nil {
		t.Fatalf("marshalSnapshotEntryJSON() error = %v", err)
	}
	if got, want := string(data), `{"key":"profile","type":"string","string":"ivi"}`; got != want {
		t.Fatalf("marshalSnapshotEntryJSON() = %q, want %q", got, want)
	}
	for _, field := range []string{"map", "slice", "set", "priority_queue"} {
		if bytes.Contains(data, []byte(field)) {
			t.Fatalf("marshalSnapshotEntryJSON() = %q, want no unrelated %s field", data, field)
		}
	}
}

func TestWriteSnapshotJSONFieldStreamsCompactValue(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSnapshotJSONField(&buf, "  ", "values", Slice{"alpha", json.Number("2")}, true); err != nil {
		t.Fatalf("writeSnapshotJSONField() error = %v", err)
	}
	if got, want := buf.String(), "  \"values\": [\"alpha\",2],\n"; got != want {
		t.Fatalf("writeSnapshotJSONField() = %q, want %q", got, want)
	}
	if err := writeSnapshotJSONField(&buf, "  ", "bad", make(chan int), false); err == nil {
		t.Fatal("writeSnapshotJSONField(unsupported) error = nil, want error")
	}
}

func TestWriteFileAtomicCleansTemporaryFileOnRenameError(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target.json")
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	if err := writeFileAtomic(targetDir, []byte("payload")); err == nil {
		t.Fatal("writeFileAtomic(directory target) error = nil, want error")
	}
	assertNoAtomicTempFiles(t, dir, "target.json")
	if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
		t.Fatalf("target directory = %v/%v, want existing directory", info, err)
	}
}

func TestSnapshotRoundTripRestoresKeyStats(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2050, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("hot", "value")
	now = now.Add(time.Second)
	if got := ht.GetString("hot"); got != "value" {
		t.Fatalf("GetString(hot) = %q, want value", got)
	}
	now = now.Add(time.Second)
	if got := ht.GetCounter("hot"); got != 0 {
		t.Fatalf("GetCounter(hot) = %d, want 0", got)
	}
	want, ok := ht.StatsForKey("hot")
	if !ok {
		t.Fatal("StatsForKey(hot) = false, want true")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(time.Hour) }
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	got, ok := loaded.StatsForKey("hot")
	if !ok {
		t.Fatal("loaded StatsForKey(hot) = false, want true")
	}
	if got != want {
		t.Fatalf("loaded key stats = %#v, want %#v", got, want)
	}
}

func TestLoadSnapshotWithoutKeyStatsDoesNotInventStats(t *testing.T) {
	ht := newTestTrie(t)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "legacy", Type: "string", String: "value"},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if stats, ok := ht.StatsForKey("legacy"); ok {
		t.Fatalf("StatsForKey(legacy) = %#v, true; want false", stats)
	}
	if got := ht.GetString("legacy"); got != "value" {
		t.Fatalf("legacy value = %q, want value", got)
	}
}

func TestSnapshotRoundTripRestoresLargeBytesToDisk(t *testing.T) {
	ht := newTestTrie(t)
	payload := testPayload(DiskBytesThreshold + 1)
	ht.UpsertBytes("large", payload)

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	hval := loaded.Get("large")
	if !hval.OnDisk() {
		t.Fatalf("large snapshot value metadata = %+v, want on disk", hval)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("large bytes changed after snapshot restore")
	}
}

func TestSaveSnapshotStreamsOnDiskBytesWithMetadata(t *testing.T) {
	now := time.Unix(6200, 0)
	expiresAt := now.Add(time.Hour)
	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	payload := testPayload(DiskBytesThreshold + 1)

	ht.UpsertBytes("large", payload)
	if got := ht.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("GetBytes(large) = %q, want payload", got)
	}
	if !ht.ExpireAt("large", expiresAt) {
		t.Fatal("ExpireAt(large) = false, want true")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if hval := loaded.Get("large"); !hval.OnDisk() || !hval.HasTtl() {
		t.Fatalf("loaded large metadata = %+v, want on-disk bytes with TTL", hval)
	}
	stats, ok := loaded.StatsForKey("large")
	if !ok || stats.Reads == 0 || stats.Writes == 0 {
		t.Fatalf("loaded large stats = %#v/%v, want persisted read/write stats", stats, ok)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("loaded large bytes changed")
	}
}

func TestPrepareSnapshotBytesOperationStreamsLargePayload(t *testing.T) {
	smallPayload := []byte("small")
	small, err := prepareSnapshotBytesOperation(snapshotEntry{
		Key:   "small",
		Type:  "bytes",
		Bytes: base64.StdEncoding.EncodeToString(smallPayload),
	})
	if err != nil {
		t.Fatalf("prepareSnapshotBytesOperation(small) error = %v", err)
	}
	if !bytes.Equal(small.bytes, smallPayload) {
		t.Fatalf("small operation bytes = %q, want %q", small.bytes, smallPayload)
	}

	largePayload := testPayload(DiskBytesThreshold + 1)
	large, err := prepareSnapshotBytesOperation(snapshotEntry{
		Key:   "large",
		Type:  "bytes",
		Bytes: base64.StdEncoding.EncodeToString(largePayload),
	})
	if err != nil {
		t.Fatalf("prepareSnapshotBytesOperation(large) error = %v", err)
	}
	if large.bytes != nil {
		t.Fatalf("large operation decoded %d bytes, want streaming encoded payload", len(large.bytes))
	}
	if !shouldStreamSnapshotBytes(large) {
		t.Fatal("shouldStreamSnapshotBytes(large) = false, want true")
	}

	commandPayload := `{"type":"bytes","bytes":"` + base64.StdEncoding.EncodeToString(largePayload) + `"}`
	commandOperation, err := commandSnapshotOperation("large-command", commandPayload)
	if err != nil {
		t.Fatalf("commandSnapshotOperation(large bytes) error = %v", err)
	}
	if commandOperation.bytes != nil {
		t.Fatalf("command snapshot operation decoded %d bytes, want streaming encoded payload", len(commandOperation.bytes))
	}
	if !shouldStreamSnapshotBytes(commandOperation) {
		t.Fatal("shouldStreamSnapshotBytes(command large) = false, want true")
	}
}

func TestLoadSnapshotSkipsExpiredEntries(t *testing.T) {
	now := time.Unix(3000, 0)
	expiredAt := now.Add(-time.Second)
	activeUntil := now.Add(time.Minute)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "expired", Type: "string", String: "old", ExpiresAt: &expiredAt},
			{Key: "active", Type: "bytes", Bytes: base64.StdEncoding.EncodeToString([]byte("live")), ExpiresAt: &activeUntil},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := ht.GetString("expired"); got != "" {
		t.Fatalf("expired snapshot entry = %q, want skipped", got)
	}
	if got := ht.GetBytes("active"); !bytes.Equal(got, []byte("live")) {
		t.Fatalf("active snapshot entry = %q, want live", got)
	}
}

func TestLoadSnapshotScansLargeExpiredHistory(t *testing.T) {
	now := time.Unix(3010, 0)
	expiredAt := now.Add(-time.Second)
	activeUntil := now.Add(time.Minute)
	largeExpiredValue := strings.Repeat("x", 70*1024)
	data := snapshotFile{
		Version:         snapshotVersion,
		JournalSequence: 88,
		Entries: []snapshotEntry{
			{Key: "active", Type: "string", String: "live", ExpiresAt: &activeUntil},
		},
	}
	for idx := 0; idx < 128; idx++ {
		data.Entries = append(data.Entries, snapshotEntry{
			Key:       "expired",
			Type:      "string",
			String:    largeExpiredValue,
			ExpiresAt: &expiredAt,
		})
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	metadata, err := ht.LoadSnapshotWithMetadata(path)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 88 {
		t.Fatalf("journal sequence = %d, want 88", metadata.JournalSequence)
	}
	if got := ht.GetString("active"); got != "live" {
		t.Fatalf("active snapshot entry = %q, want live", got)
	}
	if ht.Exists("expired") {
		t.Fatal("expired snapshot history left expired key present")
	}
	if got := ht.Size(); got != 1 {
		t.Fatalf("snapshot size after load = %d, want active key only", got)
	}
}

func TestLoadSnapshotUsesSingleExpirationClock(t *testing.T) {
	base := time.Unix(3025, 0)
	expiresAt := base.Add(time.Second)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "soon", Type: "string", String: "value", ExpiresAt: &expiresAt},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	clockReads := 0
	ht.now = func() time.Time {
		clockReads++
		if clockReads == 1 {
			return base
		}
		return base.Add(2 * time.Second)
	}
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if clockReads != 1 {
		t.Fatalf("LoadSnapshot clock reads = %d, want one captured load time", clockReads)
	}
	ht.now = func() time.Time { return base }
	if got := ht.GetString("soon"); got != "value" {
		t.Fatalf("soon after snapshot load = %q, want value", got)
	}
}

func TestLoadSnapshotRemovesKeysMissingFromSnapshot(t *testing.T) {
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "keep", Type: "string", String: "value"},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("stale", "old")
	ht.UpsertBytes("stale-large", testPayload(DiskBytesThreshold+1))
	staleValue := ht.Get("stale-large")
	stalePath := ht.disks.paths[staleValue.Index]
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := ht.GetString("keep"); got != "value" {
		t.Fatalf("keep after snapshot load = %q, want value", got)
	}
	if got := ht.GetString("stale"); got != "" {
		t.Fatalf("stale after snapshot load = %q, want empty", got)
	}
	if got := ht.GetBytes("stale-large"); got != nil {
		t.Fatalf("stale-large after snapshot load = %q, want nil", got)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale disk file Stat() error = %v, want not exist", err)
	}
}

func TestLoadSnapshotStreamsMissingKeyCleanup(t *testing.T) {
	const keepEntries = 16
	large := strings.Repeat("x", 70*1024)
	entries := make([]snapshotEntry, 0, keepEntries)
	for idx := 0; idx < keepEntries; idx++ {
		entries = append(entries, snapshotEntry{
			Key:    fmt.Sprintf("keep:%02d", idx),
			Type:   "string",
			String: large,
		})
	}
	payload, err := json.Marshal(snapshotFile{Version: snapshotVersion, Entries: entries})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	for idx := 0; idx < keepEntries; idx++ {
		ht.UpsertString(fmt.Sprintf("keep:%02d", idx), "old")
		ht.UpsertString(fmt.Sprintf("stale:%02d", idx), "old")
	}
	ht.UpsertBytes("stale-large", testPayload(DiskBytesThreshold+1))
	staleValue := ht.Get("stale-large")
	stalePath := ht.disks.paths[staleValue.Index]

	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := ht.Size(); got != len(entries) {
		t.Fatalf("size after snapshot load = %d, want %d", got, len(entries))
	}
	if got := ht.GetString("keep:15"); got != large {
		t.Fatalf("keep:15 after snapshot load length = %d, want %d", len(got), len(large))
	}
	for idx := 0; idx < keepEntries; idx++ {
		if got := ht.GetString(fmt.Sprintf("stale:%02d", idx)); got != "" {
			t.Fatalf("stale:%02d after snapshot load = %q, want empty", idx, got)
		}
	}
	if got := ht.GetBytes("stale-large"); got != nil {
		t.Fatalf("stale-large after snapshot load = %q, want nil", got)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale disk file Stat() error = %v, want not exist", err)
	}
}

func TestDeleteKeysNotInBatchesHandlesEmptyKeyCursor(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "stale-empty")
	ht.UpsertString("keep", "value")
	ht.UpsertString("stale:1", "old")
	ht.UpsertString("stale:2", "old")

	ht.mu.Lock()
	ht.deleteKeysNotInBatchesLocked(map[string]bool{"keep": true}, time.Time{}, 1)
	ht.mu.Unlock()

	if got := ht.Keys(true); !reflect.DeepEqual(got, []string{"keep"}) {
		t.Fatalf("keys after batched missing-key cleanup = %#v, want keep only", got)
	}
}

func TestLoadSnapshotCleansCreatedKeysAfterApplyFailure(t *testing.T) {
	payload := testPayload(DiskBytesThreshold + 1)
	encoded := base64.StdEncoding.EncodeToString(payload)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "a-created", Type: "bytes", Bytes: encoded},
			{Key: "b-blocked", Type: "bytes", Bytes: encoded},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	firstPath := ht.disks.pathFor(0)
	blockedPath := ht.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want blocked disk write error")
	}
	if ht.Exists("a-created") {
		t.Fatal("failed snapshot load left created key a-created")
	}
	if ht.Exists("b-blocked") {
		t.Fatal("failed snapshot load left blocked key b-blocked")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after failed snapshot load = %q, want keep", got)
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("created disk file Stat() error = %v, want not exist", err)
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed snapshot load = %d, want 0", got)
	}
}

func TestLoadSnapshotRollsBackExistingKeysAfterApplyFailure(t *testing.T) {
	payload := testPayload(DiskBytesThreshold + 1)
	encoded := base64.StdEncoding.EncodeToString(payload)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "a-existing", Type: "bytes", Bytes: encoded},
			{Key: "b-blocked", Type: "bytes", Bytes: encoded},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("a-existing", "keep")
	firstPath := ht.disks.pathFor(0)
	blockedPath := ht.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want blocked disk write error")
	}
	if got := ht.GetString("a-existing"); got != "keep" {
		t.Fatalf("a-existing after failed snapshot load = %q, want keep", got)
	}
	if ht.Exists("b-blocked") {
		t.Fatal("failed snapshot load left blocked key b-blocked")
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("rolled-back disk file Stat() error = %v, want not exist", err)
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed snapshot rollback = %d, want 0", got)
	}
}

func TestLoadSnapshotStreamsOperationsWithRollbackOnApplyFailure(t *testing.T) {
	large := strings.Repeat("x", 70*1024)
	entries := []snapshotEntry{
		{Key: "existing", Type: "string", String: "changed"},
	}
	for idx := 0; idx < 32; idx++ {
		entries = append(entries, snapshotEntry{
			Key:    fmt.Sprintf("created:%02d", idx),
			Type:   "string",
			String: large,
		})
	}
	entries = append(entries, snapshotEntry{
		Key:   "blocked",
		Type:  "bytes",
		Bytes: base64.StdEncoding.EncodeToString(testPayload(DiskBytesThreshold + 1)),
	})
	raw, err := json.Marshal(snapshotFile{Version: snapshotVersion, Entries: entries})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	blockedPath := ht.disks.pathFor(0)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want blocked disk write error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after failed snapshot load = %q, want keep", got)
	}
	for idx := 0; idx < 32; idx++ {
		key := fmt.Sprintf("created:%02d", idx)
		if ht.Exists(key) {
			t.Fatalf("failed snapshot load left created key %s", key)
		}
	}
	if ht.Exists("blocked") {
		t.Fatal("failed snapshot load left blocked key")
	}
	if got := len(ht.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed snapshot rollback = %d, want 0", got)
	}
}

func TestLoadSnapshotSchedulesExpirationForVacuum(t *testing.T) {
	now := time.Unix(3050, 0)
	expiresAt := now.Add(time.Minute)
	data := snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: "ttl", Type: "string", String: "value", ExpiresAt: &expiresAt},
		},
	}
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	now = now.Add(2 * time.Minute)
	if got := ht.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() after snapshot load = %d, want 1", got)
	}
	if got := ht.GetString("ttl"); got != "" {
		t.Fatalf("ttl after vacuum = %q, want empty", got)
	}
}

func TestLoadSnapshotAcceptsEmptyCollectionPayloads(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"map","type":"map","map":{}},{"key":"slice","type":"slice","slice":[]},{"key":"set","type":"set","set":[]},{"key":"priority","type":"priority_queue","priority_queue":[]}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	if err := ht.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if got, ok, err := ht.GetMapChecked("map"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetMapChecked(map) = %#v/%v/%v, want empty map", got, ok, err)
	}
	if got, ok, err := ht.GetSliceChecked("slice"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetSliceChecked(slice) = %#v/%v/%v, want empty slice", got, ok, err)
	}
	if got, ok, err := ht.GetSetChecked("set"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetSetChecked(set) = %#v/%v/%v, want empty set", got, ok, err)
	}
	if got, ok, err := ht.GetPriorityQueueChecked("priority"); err != nil || !ok || got == nil || len(got) != 0 {
		t.Fatalf("GetPriorityQueueChecked(priority) = %#v/%v/%v, want empty priority queue", got, ok, err)
	}
}

func TestLoadSnapshotRejectsInvalidInput(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()

	for name, payload := range map[string]string{
		"broken":           `{broken`,
		"version":          `{"version":999,"entries":[]}`,
		"type":             `{"version":1,"entries":[{"key":"bad","type":"unknown"}]}`,
		"missing-key":      `{"version":1,"entries":[{"type":"string","string":"value"}]}`,
		"null-key":         `{"version":1,"entries":[{"key":null,"type":"string","string":"value"}]}`,
		"map-missing":      `{"version":1,"entries":[{"key":"bad","type":"map"}]}`,
		"map-null":         `{"version":1,"entries":[{"key":"bad","type":"map","map":null}]}`,
		"slice-missing":    `{"version":1,"entries":[{"key":"bad","type":"slice"}]}`,
		"set-missing":      `{"version":1,"entries":[{"key":"bad","type":"set"}]}`,
		"priority-missing": `{"version":1,"entries":[{"key":"bad","type":"priority_queue"}]}`,
		"priority-null":    `{"version":1,"entries":[{"key":"bad","type":"priority_queue","priority_queue":null}]}`,
		"bytes-invalid":    `{"version":1,"entries":[{"key":"bad","type":"bytes","bytes":"not-base64!!!"}]}`,
		"entry-duplicate":  `{"version":1,"entries":[{"key":"bad","type":"string","string":"first","string":"second"}]}`,
		"entries-missing":  `{"version":1}`,
		"entries-null":     `{"version":1,"entries":null}`,
		"entries-object":   `{"version":1,"entries":{}}`,
		"trailing":         `{"version":1,"entries":[]} trailing`,
	} {
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
		if err := ht.LoadSnapshot(path); err == nil {
			t.Fatalf("LoadSnapshot(%s) error = nil, want error", name)
		}
	}
}

func TestDecodeSnapshotFileJSONReaderStreamsEntries(t *testing.T) {
	payload := `{"version":1,"journal_sequence":7,"entries":[{"key":"name","type":"string","string":"ivi"},{"key":"views","type":"counter","counter":42}]}`
	snapshot, err := decodeSnapshotFileJSONReader(iotest.OneByteReader(strings.NewReader(payload)))
	if err != nil {
		t.Fatalf("decodeSnapshotFileJSONReader() error = %v", err)
	}
	if snapshot.Version != snapshotVersion || snapshot.JournalSequence != 7 {
		t.Fatalf("snapshot metadata = version %d sequence %d, want %d/7", snapshot.Version, snapshot.JournalSequence, snapshotVersion)
	}
	if len(snapshot.Entries) != 2 {
		t.Fatalf("snapshot entries = %d, want 2", len(snapshot.Entries))
	}
	if snapshot.Entries[0].Key != "name" || snapshot.Entries[0].String != "ivi" {
		t.Fatalf("first entry = %#v, want restored string entry", snapshot.Entries[0])
	}
	if snapshot.Entries[1].Key != "views" || snapshot.Entries[1].Counter != 42 {
		t.Fatalf("second entry = %#v, want restored counter entry", snapshot.Entries[1])
	}
}

func TestScanSnapshotFileJSONReaderVisitsEntries(t *testing.T) {
	payload := `{"version":1,"journal_sequence":9,"entries":[{"key":"name","type":"string","string":"ivi"},{"key":"views","type":"counter","counter":42}]}`
	keys := []string{}
	metadata, err := scanSnapshotFileJSONReader(iotest.OneByteReader(strings.NewReader(payload)), func(entry snapshotEntry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil {
		t.Fatalf("scanSnapshotFileJSONReader() error = %v", err)
	}
	if metadata.Version != snapshotVersion || metadata.JournalSequence != 9 {
		t.Fatalf("snapshot metadata = version %d sequence %d, want %d/9", metadata.Version, metadata.JournalSequence, snapshotVersion)
	}
	if !reflect.DeepEqual(keys, []string{"name", "views"}) {
		t.Fatalf("visited keys = %#v, want name/views", keys)
	}
}

func TestSnapshotEntryHasKeyHandlesLargeEntries(t *testing.T) {
	payload := `{"type":"string","string":"` + strings.Repeat("x", 70*1024) + `","key":"name"}`
	hasKey, err := snapshotEntryHasKey([]byte(payload))
	if err != nil {
		t.Fatalf("snapshotEntryHasKey(large) error = %v", err)
	}
	if !hasKey {
		t.Fatal("snapshotEntryHasKey(large) = false, want true")
	}

	hasKey, err = snapshotEntryHasKey([]byte(`{"type":"string","string":"value"}`))
	if err != nil {
		t.Fatalf("snapshotEntryHasKey(missing) error = %v", err)
	}
	if hasKey {
		t.Fatal("snapshotEntryHasKey(missing) = true, want false")
	}

	hasKey, err = snapshotEntryHasKey([]byte(`{"key":null,"type":"string","string":"value"}`))
	if err == nil || !strings.Contains(err.Error(), "key must be a string") {
		t.Fatalf("snapshotEntryHasKey(null) error = %v, want key string error", err)
	}
	if hasKey {
		t.Fatal("snapshotEntryHasKey(null) = true, want false")
	}
}

func TestDecodeSnapshotEntryJSONRequiredKeyStreamsEntryFields(t *testing.T) {
	entry, err := decodeSnapshotEntryJSONRequiredKey([]byte(`{"key":"","type":"string","string":"blank"}`), true)
	if err != nil {
		t.Fatalf("decodeSnapshotEntryJSONRequiredKey(empty key) error = %v", err)
	}
	if entry.Key != "" || entry.String != "blank" {
		t.Fatalf("decoded empty-key entry = %#v, want blank string entry", entry)
	}

	entry, err = decodeSnapshotEntryJSONRequiredKey([]byte(`{"type":"string","string":"value"}`), false)
	if err != nil {
		t.Fatalf("decodeSnapshotEntryJSONRequiredKey(optional key) error = %v", err)
	}
	if entry.Key != "" || entry.String != "value" {
		t.Fatalf("decoded optional-key entry = %#v, want missing key accepted", entry)
	}

	for name, payload := range map[string]string{
		"missing-key":     `{"type":"string","string":"value"}`,
		"null-key":        `{"key":null,"type":"string","string":"value"}`,
		"duplicate-key":   `{"key":"name","key":"other","type":"string","string":"value"}`,
		"duplicate-value": `{"key":"name","type":"string","string":"first","string":"second"}`,
		"unknown":         `{"key":"name","type":"string","string":"value","extra":true}`,
		"trailing":        `{"key":"name","type":"string","string":"value"} true`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSnapshotEntryJSONRequiredKey([]byte(payload), true); err == nil {
				t.Fatal("decodeSnapshotEntryJSONRequiredKey() error = nil, want rejection")
			}
		})
	}
}

func TestDecodeSnapshotFileJSONReaderRejectsTopLevelAmbiguity(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown":   `{"version":1,"entries":[],"extra":true}`,
		"duplicate": `{"version":1,"entries":[],"version":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSnapshotFileJSONReader(strings.NewReader(payload)); err == nil {
				t.Fatal("decodeSnapshotFileJSONReader() error = nil, want rejection")
			}
		})
	}
}

func TestLoadSnapshotRejectsMissingEntriesWithoutMutation(t *testing.T) {
	for name, payload := range map[string]string{
		"missing": `{"version":1}`,
		"null":    `{"version":1,"entries":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.json")
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			ht := newTestTrie(t)
			ht.UpsertString("existing", "keep")
			if err := ht.LoadSnapshot(path); err == nil {
				t.Fatal("LoadSnapshot() error = nil, want missing entries error")
			}
			if got := ht.GetString("existing"); got != "keep" {
				t.Fatalf("existing after rejected snapshot = %q, want keep", got)
			}
			if got := ht.Size(); got != 1 {
				t.Fatalf("size after rejected snapshot = %d, want 1", got)
			}
		})
	}
}

func TestLoadSnapshotRejectsInvalidCollectionsWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"bad","type":"map"}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want invalid collection payload error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected snapshot = %q, want keep", got)
	}
	if ht.Exists("bad") {
		t.Fatal("invalid snapshot created bad key")
	}
}

func TestLoadSnapshotRejectsInvalidBytesWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"bad","type":"bytes","bytes":"not-base64!!!"}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want invalid bytes payload error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected snapshot = %q, want keep", got)
	}
	if ht.Exists("bad") {
		t.Fatal("invalid snapshot created bad bytes key")
	}
}

func TestLoadSnapshotRejectsDuplicateActiveKeysWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"dup","type":"string","string":"first"},{"key":"dup","type":"string","string":"second"}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "duplicate active key") {
		t.Fatalf("LoadSnapshot(duplicate key) error = %v, want duplicate active key error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected duplicate snapshot = %q, want keep", got)
	}
	if ht.Exists("dup") {
		t.Fatal("duplicate snapshot created dup key")
	}
}

func TestSnapshotActiveKeysSortsAndTracksSeenKeysCompactly(t *testing.T) {
	active, err := newSnapshotActiveKeys([]string{"zeta", "", "alpha"})
	if err != nil {
		t.Fatalf("newSnapshotActiveKeys() error = %v", err)
	}
	if want := []string{"", "alpha", "zeta"}; !reflect.DeepEqual(active.keys, want) {
		t.Fatalf("active keys = %#v, want %#v", active.keys, want)
	}
	if len(active.seen) != 1 {
		t.Fatalf("seen words = %d, want 1 compact word", len(active.seen))
	}
	if err := active.markSeen("alpha"); err != nil {
		t.Fatalf("markSeen(alpha) error = %v", err)
	}
	if err := active.markSeen("missing"); !errors.Is(err, errSnapshotChangedDuringLoad) {
		t.Fatalf("markSeen(missing) error = %v, want snapshot changed", err)
	}
	if err := active.markSeen("alpha"); !errors.Is(err, errSnapshotDuplicateActiveKey) {
		t.Fatalf("markSeen(alpha duplicate) error = %v, want duplicate active key", err)
	}
}

func TestSnapshotActiveKeysRejectsDuplicateEmptyKey(t *testing.T) {
	if _, err := newSnapshotActiveKeys([]string{"", ""}); !errors.Is(err, errSnapshotDuplicateActiveKey) {
		t.Fatalf("newSnapshotActiveKeys(duplicate empty) error = %v, want duplicate active key", err)
	}
}

func TestLoadSnapshotRejectsTooLongKey(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	data, err := json.Marshal(snapshotFile{
		Version: snapshotVersion,
		Entries: []snapshotEntry{
			{Key: strings.Repeat("k", maxHATTrieKeyLength+1), Type: "string", String: "bad"},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("LoadSnapshot(too-long key) error = %v, want key length error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing value after rejected snapshot = %q, want keep", got)
	}
	if got := ht.Size(); got != 1 {
		t.Fatalf("size after rejected snapshot = %d, want 1", got)
	}
}

func TestValidateSnapshotEntryRejectsUnsupportedCollectionValues(t *testing.T) {
	unsupported := func() {}
	tests := []snapshotEntry{
		{Key: "map", Type: "map", Map: Map{"bad": unsupported}},
		{Key: "slice", Type: "slice", Slice: Slice{unsupported}},
		{Key: "set", Type: "set", Set: Set{unsupported}},
		{Key: "priority", Type: "priority_queue", PriorityQueue: []priorityQueueItem{{Value: unsupported}}},
	}
	for _, entry := range tests {
		if _, err := validateSnapshotEntry(entry); err == nil {
			t.Fatalf("validateSnapshotEntry(%s) error = nil, want unsupported value error", entry.Type)
		}
	}
}

func TestValidateSnapshotEntryRejectsInconsistentKeyStats(t *testing.T) {
	entry := snapshotEntry{
		Key:    "bad-stats",
		Type:   "string",
		String: "value",
		Stats: &KeyStats{
			Reads:  1,
			Hits:   2,
			Misses: 0,
		},
	}
	if _, err := validateSnapshotEntry(entry); err == nil || !strings.Contains(err.Error(), "key stats reads must equal hits plus misses") {
		t.Fatalf("validateSnapshotEntry(inconsistent key stats) error = %v, want key stats read count error", err)
	}
}

func TestLoadSnapshotRejectsInconsistentKeyStatsWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"bad-stats","type":"string","string":"value","stats":{"reads":1,"hits":2,"misses":0}}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil || !strings.Contains(err.Error(), "key stats reads must equal hits plus misses") {
		t.Fatalf("LoadSnapshot(inconsistent key stats) error = %v, want key stats read count error", err)
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected key stats snapshot = %q, want keep", got)
	}
	if ht.Exists("bad-stats") {
		t.Fatal("invalid key stats snapshot created bad-stats key")
	}
}

func TestValidateSnapshotEntryRejectsCorruptPriorityQueueSequences(t *testing.T) {
	tests := []snapshotEntry{
		{
			Key:  "duplicate",
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: 7, Value: "first"},
				{Priority: 1, Sequence: 7, Value: "second"},
			},
		},
		{
			Key:  "overflow",
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: ^uint64(0), Value: "last"},
			},
		},
	}
	for _, entry := range tests {
		if _, err := validateSnapshotEntry(entry); err == nil {
			t.Fatalf("validateSnapshotEntry(%s) error = nil, want corrupt priority queue sequence error", entry.Key)
		}
	}
}

func TestLoadSnapshotRejectsCorruptPriorityQueueWithoutMutation(t *testing.T) {
	payload := []byte(`{"version":1,"entries":[{"key":"existing","type":"string","string":"changed"},{"key":"jobs","type":"priority_queue","priority_queue":[{"priority":1,"sequence":2,"value":"first"},{"priority":1,"sequence":2,"value":"second"}]}]}`)
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	ht := newTestTrie(t)
	ht.UpsertString("existing", "keep")
	if err := ht.LoadSnapshot(path); err == nil {
		t.Fatal("LoadSnapshot() error = nil, want corrupt priority queue error")
	}
	if got := ht.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected snapshot = %q, want keep", got)
	}
	if ht.Exists("jobs") {
		t.Fatal("invalid priority queue snapshot created jobs key")
	}
}

func TestSaveSnapshotRejectsUnsupportedJSONValues(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertMap("bad", Map{"ch": make(chan int)})

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	if err := writeFileAtomic(path, []byte("previous")); err != nil {
		t.Fatalf("writeFileAtomic(previous) error = %v", err)
	}
	if err := ht.SaveSnapshot(path); err == nil {
		t.Fatal("SaveSnapshot() error = nil, want unsupported JSON value error")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "previous" {
		t.Fatalf("snapshot target after failed save = %q, want previous", data)
	}
	assertNoAtomicTempFiles(t, dir, "snapshot.json")
}

func assertNoAtomicTempFiles(t *testing.T, dir string, base string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, base+".tmp-*"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files remain: %v", matches)
	}
}
