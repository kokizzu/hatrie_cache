package hatriecache

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"testing"
)

func TestFixedStructuredBinaryRecordsMatchBufferedEncoding(t *testing.T) {
	for _, entry := range benchmarkFixedBinaryEntries() {
		t.Run(entry.Type, func(t *testing.T) {
			got, err := marshalLevelDBEntryBinary(entry)
			if err != nil {
				t.Fatalf("marshalLevelDBEntryBinary() error = %v", err)
			}
			want, err := marshalFixedLevelDBEntryBinaryBufferedForTest(entry)
			if err != nil {
				t.Fatalf("marshalFixedLevelDBEntryBinaryBufferedForTest() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("binary %s storage record differs from buffered encoding", entry.Type)
			}
			decoded, err := decodeLevelDBEntry(got)
			if err != nil {
				t.Fatalf("decodeLevelDBEntry() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, entry) {
				t.Fatalf("decoded %s entry = %#v, want %#v", entry.Type, decoded, entry)
			}
		})
	}
}

func TestFixedStructuredReplicationValuesMatchBufferedEncoding(t *testing.T) {
	prefix := []byte("fixed-replication-prefix")
	for _, entry := range benchmarkFixedBinaryEntries() {
		t.Run(entry.Type, func(t *testing.T) {
			gotDestination := make([]byte, len(prefix), 32768)
			wantDestination := make([]byte, len(prefix), 32768)
			copy(gotDestination, prefix)
			copy(wantDestination, prefix)
			got, err := appendReplicationValueBinary(gotDestination, entry)
			if err != nil {
				t.Fatalf("appendReplicationValueBinary() error = %v", err)
			}
			want, err := appendReplicationValueBinaryBufferedForTest(wantDestination, entry)
			if err != nil {
				t.Fatalf("appendReplicationValueBinaryBufferedForTest() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("binary %s replication value differs from buffered encoding", entry.Type)
			}
			if &got[0] != &gotDestination[0] {
				t.Fatal("appendReplicationValueBinary() allocated a new destination")
			}
			decoded, err := unmarshalReplicationValueBinary(entry.Key, got[len(prefix):])
			if err != nil {
				t.Fatalf("unmarshalReplicationValueBinary() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, entry) {
				t.Fatalf("decoded %s replication entry = %#v, want %#v", entry.Type, decoded, entry)
			}
		})
	}
}

func TestFixedStructuredBinaryRecordsAcceptBase64Newlines(t *testing.T) {
	entry := benchmarkFixedBinaryEntries()[0]
	encoded := entry.BloomFilter.Bits
	entry.BloomFilter.Bits = encoded[:8] + "\r\n" + encoded[8:]

	got, err := marshalLevelDBEntryBinary(entry)
	if err != nil {
		t.Fatalf("marshalLevelDBEntryBinary() error = %v", err)
	}
	entry.BloomFilter.Bits = encoded
	want, err := marshalLevelDBEntryBinary(entry)
	if err != nil {
		t.Fatalf("marshalLevelDBEntryBinary(canonical) error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("base64 newlines changed the fixed binary record")
	}
}

func TestFixedStructuredReplicationErrorPreservesDestination(t *testing.T) {
	entry := benchmarkFixedBinaryEntries()[0]
	entry.BloomFilter.Bits = "not-base64!!!"
	destination := make([]byte, 16, 32768)
	copy(destination, "fixed-prefix")
	want := append([]byte(nil), destination...)

	got, err := appendReplicationValueBinary(destination, entry)
	if err == nil {
		t.Fatal("appendReplicationValueBinary() error = nil, want malformed base64 error")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("destination after error = %q, want %q", got, want)
	}
	if &got[0] != &destination[0] {
		t.Fatal("appendReplicationValueBinary() replaced the destination on error")
	}
}

func marshalFixedLevelDBEntryBinaryBufferedForTest(entry snapshotEntry) ([]byte, error) {
	value, err := prepareLevelDBBinaryEntryValue(entry)
	if err != nil {
		return nil, err
	}
	capacity, err := levelDBBinaryRecordCapacityForValue(entry.Key, entry.Type, value.encodedSize, entry.ExpiresAt, entry.Stats)
	if err != nil {
		return nil, err
	}
	writer := newLevelDBBinaryWriterWithCapacity(capacity)
	writer.writeString(entry.Key)
	writer.writeString(entry.Type)
	writer.writePreparedSnapshotEntryValue(value)
	writer.writeTimePtr(entry.ExpiresAt)
	writer.writeKeyStatsPtr(entry.Stats)
	return writer.bytes(), nil
}

func BenchmarkFixedStructuredStorageEncode(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()
	recordBytes := fixedBinaryStorageRecordBytes(b, entries)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		for _, entry := range entries {
			if _, err := marshalLevelDBEntryBinary(entry); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkFixedStructuredReplicationAppendReuse(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()
	recordBytes := fixedBinaryReplicationRecordBytes(b, entries)
	buffer := make([]byte, 0, recordBytes)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		for _, entry := range entries {
			var err error
			buffer, err = appendReplicationValueBinary(buffer, entry)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCanonicalRawFixedStorageEncode(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()[:5]
	recordBytes := fixedBinaryStorageRecordBytes(b, entries)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		for _, entry := range entries {
			if _, err := marshalLevelDBEntryBinary(entry); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCanonicalRawFixedReplicationAppendReuse(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()[:5]
	recordBytes := fixedBinaryReplicationRecordBytes(b, entries)
	buffer := make([]byte, 0, recordBytes)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		for _, entry := range entries {
			var err error
			buffer, err = appendReplicationValueBinary(buffer, entry)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func fixedBinaryStorageRecordBytes(tb testing.TB, entries []snapshotEntry) int {
	tb.Helper()
	total := 0
	for _, entry := range entries {
		data, err := marshalLevelDBEntryBinary(entry)
		if err != nil {
			tb.Fatal(err)
		}
		total += len(data)
	}
	return total
}

func fixedBinaryReplicationRecordBytes(tb testing.TB, entries []snapshotEntry) int {
	tb.Helper()
	total := 0
	for _, entry := range entries {
		data, err := marshalReplicationValueBinary(entry)
		if err != nil {
			tb.Fatal(err)
		}
		total += len(data)
	}
	return total
}

func benchmarkFixedBinaryEntries() []snapshotEntry {
	encode := base64.StdEncoding.EncodeToString
	bloomBits := make([]byte, 256)
	counters := make([]byte, 1024)
	registers := make([]byte, 1024)
	cuckooFingerprints := make([]byte, 512)
	xorFingerprints := make([]byte, 510)
	bitmapBits := make([]byte, roaringBitmapBitmapWords*8)
	bitmapBits[0] = 1
	sparseBits := make([]byte, sparseBitsetBitmapWords*8)
	sparseBits[0] = 1
	fenwickValues := make([]int64, 257)
	quantiles := make([]quantileSketchSample, 128)
	for index := range quantiles {
		quantiles[index] = quantileSketchSample{Value: float64(index), Gap: 1}
	}
	return []snapshotEntry{
		{Key: "fixed:bloom", Type: "bloom_filter", BloomFilter: &bloomFilterSnapshot{BitCount: 2048, HashCount: 4, Insertions: 0, Bits: encode(bloomBits)}},
		{Key: "fixed:cms", Type: "count_min_sketch", CountMinSketch: &countMinSketchSnapshot{Width: 64, Depth: 4, Counters: encode(counters)}},
		{Key: "fixed:hll", Type: "hyperloglog", HyperLogLog: &hyperLogLogSnapshot{Precision: 10, Registers: encode(registers)}},
		{Key: "fixed:cuckoo", Type: "cuckoo_filter", CuckooFilter: &cuckooFilterSnapshot{BucketCount: 64, BucketSize: 4, FingerprintBits: 16, Fingerprints: encode(cuckooFingerprints)}},
		{Key: "fixed:xor", Type: "xor_filter", XorFilter: &xorFilterSnapshot{ExpectedItems: 256, Built: true, Items: 128, Seed: 9, BlockLength: 170, Fingerprints: encode(xorFingerprints)}},
		{Key: "fixed:roaring", Type: "roaring_bitmap", RoaringBitmap: &roaringBitmapSnapshot{Cardinality: 1, Containers: []roaringBitmapContainerSnapshot{{Key: 1, Kind: roaringBitmapContainerKindBits, Cardinality: 1, Bits: encode(bitmapBits)}}}},
		{Key: "fixed:sparse", Type: "sparse_bitset", SparseBitset: &sparseBitsetSnapshot{Cardinality: 1, Containers: []sparseBitsetContainerSnapshot{{Key: 1, Kind: sparseBitsetContainerKindBits, Cardinality: 1, Bits: encode(sparseBits)}}}},
		{Key: "fixed:fenwick", Type: "fenwick_tree", FenwickTree: &fenwickTreeSnapshot{Size: 256, Tree: fenwickValues}},
		{Key: "fixed:quantile", Type: "quantile_sketch", QuantileSketch: &quantileSketchSnapshot{Epsilon: 0.01, Count: 128, Summary: quantiles}},
	}
}

func TestBenchmarkFixedBinaryEntriesHaveUniqueKeys(t *testing.T) {
	seen := make(map[string]struct{})
	for _, entry := range benchmarkFixedBinaryEntries() {
		if _, ok := seen[entry.Key]; ok {
			t.Fatalf("duplicate fixed fixture key %q", entry.Key)
		}
		seen[entry.Key] = struct{}{}
	}
	if len(seen) != 9 {
		t.Fatalf("fixed fixture entries = %d, want 9", len(seen))
	}
}
