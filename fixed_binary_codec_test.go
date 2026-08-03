package hatriecache

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"testing"
	"time"
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

func TestCanonicalFixedStructuredReplicationValuesMatchValidatedEncoding(t *testing.T) {
	prefix := []byte("canonical-fixed-prefix")
	for _, entry := range benchmarkFixedBinaryEntries()[:7] {
		t.Run(entry.Type, func(t *testing.T) {
			gotDestination := make([]byte, len(prefix), 32768)
			wantDestination := make([]byte, len(prefix), 32768)
			copy(gotDestination, prefix)
			copy(wantDestination, prefix)
			got, err := appendCanonicalReplicationValueBinary(gotDestination, entry)
			if err != nil {
				t.Fatalf("appendCanonicalReplicationValueBinary() error = %v", err)
			}
			want, err := appendReplicationValueBinary(wantDestination, entry)
			if err != nil {
				t.Fatalf("appendReplicationValueBinary() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("canonical %s replication value differs from validated encoding", entry.Type)
			}
			if &got[0] != &gotDestination[0] {
				t.Fatal("appendCanonicalReplicationValueBinary() allocated a new destination")
			}
			decoded, err := unmarshalReplicationValueBinary(entry.Key, got[len(prefix):])
			if err != nil {
				t.Fatalf("unmarshalReplicationValueBinary() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, entry) {
				t.Fatalf("decoded canonical %s entry = %#v, want %#v", entry.Type, decoded, entry)
			}
		})
	}
}

func TestCanonicalFixedStructuredReplicationFallsBackForBase64Newlines(t *testing.T) {
	entries := benchmarkFixedBinaryEntries()
	entries[0].BloomFilter.Bits = entries[0].BloomFilter.Bits[:8] + "\r\n\r\n" + entries[0].BloomFilter.Bits[8:]
	entries[5].RoaringBitmap.Containers[0].Bits = entries[5].RoaringBitmap.Containers[0].Bits[:8] + "\r\n\r\n" + entries[5].RoaringBitmap.Containers[0].Bits[8:]
	for _, entry := range []snapshotEntry{entries[0], entries[5]} {
		gotDestination := make([]byte, 8, 32768)
		wantDestination := make([]byte, 8, 32768)
		copy(gotDestination, "prefix::")
		copy(wantDestination, "prefix::")
		got, err := appendCanonicalReplicationValueBinary(gotDestination, entry)
		if err != nil {
			t.Fatalf("appendCanonicalReplicationValueBinary(%s) error = %v", entry.Type, err)
		}
		want, err := appendReplicationValueBinary(wantDestination, entry)
		if err != nil {
			t.Fatalf("appendReplicationValueBinary(%s) error = %v", entry.Type, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("canonical %s newline fallback differs from validated encoding", entry.Type)
		}
		if &got[0] != &gotDestination[0] {
			t.Fatalf("canonical %s newline fallback replaced the destination", entry.Type)
		}
	}
}

func TestFixedCommandDumpDirectMatchesSnapshotEncoding(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	requests := []CacheCommandRequest{
		{Command: "CREATEBF", Key: "direct:bloom", Value: "2048", Subkey: "0.001"},
		{Command: "ADDBF", Key: "direct:bloom", Value: "value"},
		{Command: "CREATECMS", Key: "direct:cms", Value: "256", Subkey: "4"},
		{Command: "INCRCMS", Key: "direct:cms", Value: "value", Subkey: "3"},
		{Command: "CREATEHLL", Key: "direct:hll", Value: "10"},
		{Command: "ADDHLL", Key: "direct:hll", Value: "value"},
		{Command: "CREATECF", Key: "direct:cuckoo", Value: "2048", Subkey: "0.001"},
		{Command: "ADDCF", Key: "direct:cuckoo", Value: "value"},
		{Command: "CREATEXF", Key: "direct:xor", Value: "64"},
		{Command: "ADDXF", Key: "direct:xor", Value: "value-a"},
		{Command: "ADDXF", Key: "direct:xor", Value: "value-b"},
		{Command: "BUILDXF", Key: "direct:xor"},
		{Command: "CREATEXF", Key: "direct:xor-staged", Value: "64"},
		{Command: "ADDXF", Key: "direct:xor-staged", Value: "value"},
		{Command: "CREATEFW", Key: "direct:fenwick", Value: "128"},
		{Command: "ADDFW", Key: "direct:fenwick", Value: "32", Subkey: "3"},
		{Command: "CREATEFW", Key: "direct:fenwick-empty", Value: "128"},
		{Command: "CREATEQ", Key: "direct:quantile", Value: "0.01"},
		{Command: "ADDQ", Key: "direct:quantile", Value: "1.5"},
		{Command: "ADDQ", Key: "direct:quantile", Value: "9.5"},
	}
	for _, request := range requests {
		response := ht.ExecuteCommand(request)
		if !response.OK {
			t.Fatalf("%s %s failed: %s", request.Command, request.Key, response.Message)
		}
	}
	if !ht.Expire("direct:bloom", time.Hour) {
		t.Fatal("Expire(direct:bloom) = false")
	}
	roaringValues := make([]uint32, roaringBitmapArrayMaxSize+1)
	sparseValues := make([]uint64, sparseBitsetArrayMaxSize+1)
	for index := range roaringValues {
		roaringValues[index] = uint32(index)
		sparseValues[index] = uint64(index)
	}
	if added := ht.AddRoaringBitmap("direct:roaring", roaringValues[0], roaringValues[1:]...); added != len(roaringValues) {
		t.Fatalf("AddRoaringBitmap() = %d, want %d", added, len(roaringValues))
	}
	if added := ht.AddSparseBitset("direct:sparse", sparseValues[0], sparseValues[1:]...); added != len(sparseValues) {
		t.Fatalf("AddSparseBitset() = %d, want %d", added, len(sparseValues))
	}
	if added := ht.AddSparseBitset("direct:sparse-inline", 7); added != 1 {
		t.Fatalf("AddSparseBitset(inline) = %d, want 1", added)
	}

	for _, key := range []string{"direct:bloom", "direct:cms", "direct:hll", "direct:cuckoo", "direct:xor", "direct:xor-staged", "direct:roaring", "direct:sparse", "direct:sparse-inline", "direct:fenwick", "direct:fenwick-empty", "direct:quantile"} {
		got, ok, err := ht.commandDumpEntryBinaryWithoutStats(key)
		if err != nil || !ok {
			t.Fatalf("commandDumpEntryBinaryWithoutStats(%s) = %v/%v", key, ok, err)
		}
		ht.mu.Lock()
		hval := ht.peekCachedLocked(key)
		snapshot, snapshotErr := ht.snapshotEntryWithoutStatsLocked(Entry{Key: key, Value: hval})
		ht.mu.Unlock()
		if snapshotErr != nil {
			t.Fatalf("snapshotEntryWithoutStatsLocked(%s) error = %v", key, snapshotErr)
		}
		want, err := appendCanonicalReplicationValueBinary(nil, snapshot)
		if err != nil {
			t.Fatalf("appendCanonicalReplicationValueBinary(%s) error = %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("direct command dump %s differs from snapshot encoding", key)
		}
		decoded, err := unmarshalReplicationValueBinary(key, got)
		if err != nil {
			t.Fatalf("unmarshalReplicationValueBinary(%s) error = %v", key, err)
		}
		if snapshot.ExpiresAt != nil {
			if decoded.ExpiresAt == nil || !decoded.ExpiresAt.Equal(*snapshot.ExpiresAt) {
				t.Fatalf("decoded direct command dump %s expiry = %v, want %v", key, decoded.ExpiresAt, snapshot.ExpiresAt)
			}
			decoded.ExpiresAt = snapshot.ExpiresAt
		}
		if !reflect.DeepEqual(decoded, snapshot) {
			t.Fatalf("decoded direct command dump %s = %#v, want %#v", key, decoded, snapshot)
		}
	}
}

func TestFixedStructuredBinaryRecordsAcceptBase64Newlines(t *testing.T) {
	entry := benchmarkFixedBinaryEntries()[0]
	encoded := entry.BloomFilter.Bits
	entry.BloomFilter.Bits = encoded[:8] + "\r\n\r\n" + encoded[8:]

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
	entry.BloomFilter.Bits = "!!!!"
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

func TestFixedStructuredStorageRejectsMalformedBase64(t *testing.T) {
	entry := benchmarkFixedBinaryEntries()[0]
	entry.BloomFilter.Bits = "!!!!"
	_, wantErr := base64.StdEncoding.DecodeString(entry.BloomFilter.Bits)
	if _, err := marshalLevelDBEntryBinary(entry); err == nil {
		t.Fatal("marshalLevelDBEntryBinary() error = nil, want malformed base64 error")
	} else if err.Error() != wantErr.Error() {
		t.Fatalf("marshalLevelDBEntryBinary() error = %q, want %q", err, wantErr)
	}
}

func TestBitmapFixedStructuredBinaryRecordsAcceptBase64Newlines(t *testing.T) {
	entries := benchmarkFixedBinaryEntries()
	for _, index := range []int{5, 6} {
		entry := entries[index]
		var encoded string
		switch entry.Type {
		case "roaring_bitmap":
			encoded = entry.RoaringBitmap.Containers[0].Bits
			entry.RoaringBitmap.Containers[0].Bits = encoded[:8] + "\r\n\r\n" + encoded[8:]
		case "sparse_bitset":
			encoded = entry.SparseBitset.Containers[0].Bits
			entry.SparseBitset.Containers[0].Bits = encoded[:8] + "\r\n\r\n" + encoded[8:]
		default:
			t.Fatalf("unexpected bitmap fixture type %q", entry.Type)
		}

		got, err := marshalLevelDBEntryBinary(entry)
		if err != nil {
			t.Fatalf("marshalLevelDBEntryBinary(%s) error = %v", entry.Type, err)
		}
		canonical := benchmarkFixedBinaryEntries()[index]
		want, err := marshalLevelDBEntryBinary(canonical)
		if err != nil {
			t.Fatalf("marshalLevelDBEntryBinary(%s canonical) error = %v", entry.Type, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("base64 newlines changed the %s binary record", entry.Type)
		}
	}
}

func TestBitmapFixedStructuredStorageRejectsMalformedBase64(t *testing.T) {
	entries := benchmarkFixedBinaryEntries()
	for _, index := range []int{5, 6} {
		entry := entries[index]
		switch entry.Type {
		case "roaring_bitmap":
			entry.RoaringBitmap.Containers[0].Bits = "!!!!"
		case "sparse_bitset":
			entry.SparseBitset.Containers[0].Bits = "!!!!"
		default:
			t.Fatalf("unexpected bitmap fixture type %q", entry.Type)
		}
		_, wantErr := base64.StdEncoding.DecodeString("!!!!")
		if _, err := marshalLevelDBEntryBinary(entry); err == nil {
			t.Fatalf("marshalLevelDBEntryBinary(%s) error = nil, want malformed base64 error", entry.Type)
		} else if err.Error() != wantErr.Error() {
			t.Fatalf("marshalLevelDBEntryBinary(%s) error = %q, want %q", entry.Type, err, wantErr)
		}
	}
}

func TestBitmapFixedStructuredBinaryRecordsMatchBufferedEncodingForMixedContainers(t *testing.T) {
	fixtures := benchmarkFixedBinaryEntries()
	arrayPayload := base64.StdEncoding.EncodeToString([]byte{1, 0, 2, 0})
	entries := []snapshotEntry{
		{
			Key:  "fixed:roaring:mixed",
			Type: "roaring_bitmap",
			RoaringBitmap: &roaringBitmapSnapshot{Cardinality: 3, Containers: []roaringBitmapContainerSnapshot{
				{Key: 1, Kind: roaringBitmapContainerKindArray, Cardinality: 2, Values: arrayPayload},
				{Key: 2, Kind: roaringBitmapContainerKindBits, Cardinality: 1, Bits: fixtures[5].RoaringBitmap.Containers[0].Bits},
			}},
		},
		{
			Key:  "fixed:sparse:mixed",
			Type: "sparse_bitset",
			SparseBitset: &sparseBitsetSnapshot{Cardinality: 3, Containers: []sparseBitsetContainerSnapshot{
				{Key: 1, Kind: sparseBitsetContainerKindArray, Cardinality: 2, Values: arrayPayload},
				{Key: 2, Kind: sparseBitsetContainerKindBits, Cardinality: 1, Bits: fixtures[6].SparseBitset.Containers[0].Bits},
			}},
		},
	}

	for _, entry := range entries {
		got, err := marshalLevelDBEntryBinary(entry)
		if err != nil {
			t.Fatalf("marshalLevelDBEntryBinary(%s) error = %v", entry.Type, err)
		}
		want, err := marshalFixedLevelDBEntryBinaryBufferedForTest(entry)
		if err != nil {
			t.Fatalf("marshalFixedLevelDBEntryBinaryBufferedForTest(%s) error = %v", entry.Type, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("binary %s mixed-container record differs from buffered encoding", entry.Type)
		}
		decoded, err := decodeLevelDBEntry(got)
		if err != nil {
			t.Fatalf("decodeLevelDBEntry(%s) error = %v", entry.Type, err)
		}
		if !reflect.DeepEqual(decoded, entry) {
			t.Fatalf("decoded %s entry = %#v, want %#v", entry.Type, decoded, entry)
		}
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

func BenchmarkBitmapFixedStorageEncode(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()[5:7]
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

func BenchmarkBitmapFixedReplicationAppendReuse(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()[5:7]
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

func BenchmarkCanonicalFixedReplicationAppendReuse(b *testing.B) {
	entries := benchmarkFixedBinaryEntries()[:7]
	recordBytes := fixedBinaryReplicationRecordBytes(b, entries)
	buffer := make([]byte, 0, recordBytes)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		for _, entry := range entries {
			var err error
			buffer, err = appendCanonicalReplicationValueBinary(buffer, entry)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCanonicalFixedReplicationSnapshotPipeline(b *testing.B) {
	fixtures := benchmarkFixedBinaryEntries()
	bloom, err := newBloomFilterDataFromSnapshot(*fixtures[0].BloomFilter)
	if err != nil {
		b.Fatal(err)
	}
	roaring, err := newRoaringBitmapDataFromSnapshot(*fixtures[5].RoaringBitmap)
	if err != nil {
		b.Fatal(err)
	}
	modes := []struct {
		name   string
		append func([]byte, snapshotEntry) ([]byte, error)
	}{
		{name: "Validated", append: appendReplicationValueBinary},
		{name: "Canonical", append: appendCanonicalReplicationValueBinary},
	}
	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			bloomSnapshot := bloom.Snapshot()
			roaringSnapshot := roaring.Snapshot()
			entries := []snapshotEntry{
				{Type: "bloom_filter", BloomFilter: &bloomSnapshot},
				{Type: "roaring_bitmap", RoaringBitmap: &roaringSnapshot},
			}
			wireBytes := fixedBinaryReplicationRecordBytes(b, entries)
			buffer := make([]byte, 0, wireBytes)
			b.ReportAllocs()
			b.ReportMetric(float64(wireBytes), "wire_B/op")
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				bloomSnapshot = bloom.Snapshot()
				roaringSnapshot = roaring.Snapshot()
				entries[0].BloomFilter = &bloomSnapshot
				entries[1].RoaringBitmap = &roaringSnapshot
				buffer = buffer[:0]
				for _, entry := range entries {
					buffer, err = mode.append(buffer, entry)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkFixedCommandDumpBloomFilterReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	setupCommandFeatureBloomWithValue(b, ht)
	initial, ok, err := ht.commandDumpEntryBinaryWithoutStats("bloom:key")
	if err != nil || !ok {
		b.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
	}
	buffer := make([]byte, 0, len(initial))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(initial)), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		buffer, ok, err = ht.appendCommandDumpEntryBinaryWithoutStats(buffer, "bloom:key")
		if err != nil || !ok {
			b.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
		}
	}
}

func BenchmarkFixedCommandDumpRoaringBitmapReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	values := make([]uint32, roaringBitmapArrayMaxSize+1)
	for index := range values {
		values[index] = uint32(index)
	}
	if added := ht.AddRoaringBitmap("roaring:direct", values[0], values[1:]...); added != len(values) {
		b.Fatalf("AddRoaringBitmap() = %d, want %d", added, len(values))
	}
	initial, ok, err := ht.commandDumpEntryBinaryWithoutStats("roaring:direct")
	if err != nil || !ok {
		b.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
	}
	buffer := make([]byte, 0, len(initial))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(initial)), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		buffer, ok, err = ht.appendCommandDumpEntryBinaryWithoutStats(buffer, "roaring:direct")
		if err != nil || !ok {
			b.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
		}
	}
}

func BenchmarkFixedCommandDumpFenwickTreeReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	setupCommandFeatureFenwickTreeWithValues(b, ht)
	initial, ok, err := ht.commandDumpEntryBinaryWithoutStats("fenwick:key")
	if err != nil || !ok {
		b.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
	}
	buffer := make([]byte, 0, len(initial))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(initial)), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		buffer, ok, err = ht.appendCommandDumpEntryBinaryWithoutStats(buffer, "fenwick:key")
		if err != nil || !ok {
			b.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
		}
	}
}

func BenchmarkFixedCommandDumpQuantileSketchReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	setupCommandFeatureQuantileSketchWithValues(b, ht)
	initial, ok, err := ht.commandDumpEntryBinaryWithoutStats("quantile:key")
	if err != nil || !ok {
		b.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
	}
	buffer := make([]byte, 0, len(initial))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(initial)), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		buffer, ok, err = ht.appendCommandDumpEntryBinaryWithoutStats(buffer, "quantile:key")
		if err != nil || !ok {
			b.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
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
