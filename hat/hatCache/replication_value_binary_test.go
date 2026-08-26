package hatCache

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestReplicationValueBinaryRoundTripOmitsKeyAndStats(t *testing.T) {
	expiresAt := time.Unix(1700000000, 123)
	entry := snapshotEntry{
		Key:       "session:customer:123456789",
		Type:      "map",
		Map:       Map{"name": "alice", "active": true},
		ExpiresAt: &expiresAt,
		Stats:     &KeyStats{Reads: 99, Writes: 7},
	}

	compact, err := marshalReplicationValueBinary(entry)
	if err != nil {
		t.Fatalf("marshalReplicationValueBinary() error = %v", err)
	}
	if !replicationValueDataIsBinary(compact) {
		t.Fatalf("compact header = % x, want replication value magic", compact)
	}
	decoded, err := unmarshalReplicationValueBinary(entry.Key, compact)
	if err != nil {
		t.Fatalf("unmarshalReplicationValueBinary() error = %v", err)
	}
	want := entry
	want.Stats = nil
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded = %#v, want %#v", decoded, want)
	}

	v2, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(V2) error = %v", err)
	}
	if len(compact) >= len(v2) {
		t.Fatalf("compact bytes = %d, want smaller than V2 bytes %d", len(compact), len(v2))
	}
}

func TestAppendReplicationValueBinaryReusesDestination(t *testing.T) {
	prefix := []byte("page-prefix")
	destination := make([]byte, len(prefix), 256)
	copy(destination, prefix)
	entry := snapshotEntry{Key: "session:1", Type: "string", String: "value"}

	data, err := appendReplicationValueBinary(destination, entry)
	if err != nil {
		t.Fatalf("appendReplicationValueBinary() error = %v", err)
	}
	if string(data[:len(prefix)]) != string(prefix) {
		t.Fatalf("prefix = %q, want %q", data[:len(prefix)], prefix)
	}
	if &data[0] != &destination[0] {
		t.Fatal("appendReplicationValueBinary() allocated despite sufficient destination capacity")
	}
	decoded, err := unmarshalReplicationValueBinary(entry.Key, data[len(prefix):])
	if err != nil {
		t.Fatalf("unmarshalReplicationValueBinary() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, entry) {
		t.Fatalf("decoded = %#v, want %#v", decoded, entry)
	}
}

func TestAppendReplicationStructuredValuesMatchBufferedEncoding(t *testing.T) {
	prefix := []byte("replication-page-prefix")
	expiresAt := time.Unix(1700000000, 123)
	for _, entry := range benchmarkStructuredFallbackEntries() {
		entry.ExpiresAt = &expiresAt
		gotDestination := make([]byte, len(prefix), 1024)
		wantDestination := make([]byte, len(prefix), 1024)
		copy(gotDestination, prefix)
		copy(wantDestination, prefix)
		got, err := appendReplicationValueBinary(gotDestination, entry)
		if err != nil {
			t.Fatalf("appendReplicationValueBinary(%s) error = %v", entry.Type, err)
		}
		want, err := appendReplicationValueBinaryBufferedForTest(wantDestination, entry)
		if err != nil {
			t.Fatalf("appendReplicationValueBinaryBufferedForTest(%s) error = %v", entry.Type, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("binary %s replication value differs from buffered encoding:\n got % x\nwant % x", entry.Type, got, want)
		}
		if &got[0] != &gotDestination[0] {
			t.Fatalf("appendReplicationValueBinary(%s) allocated despite sufficient destination capacity", entry.Type)
		}
	}
}

func appendReplicationValueBinaryBufferedForTest(destination []byte, entry snapshotEntry) ([]byte, error) {
	value, err := prepareLevelDBBinaryEntryValue(entry)
	if err != nil {
		return destination, err
	}
	capacity, err := replicationValueBinaryCapacity(entry.Type, value.encodedSize, entry.ExpiresAt)
	if err != nil {
		return destination, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString(entry.Type)
	writer.writePreparedSnapshotEntryValue(value)
	writer.writeTimePtr(entry.ExpiresAt)
	return writer.bytes(), nil
}

func BenchmarkReplicationStructuredFallbackEncode(b *testing.B) {
	entries := benchmarkStructuredFallbackEntries()
	recordBytes := 0
	for _, entry := range entries {
		data, err := marshalReplicationValueBinary(entry)
		if err != nil {
			b.Fatal(err)
		}
		recordBytes += len(data)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(recordBytes), "record_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		for _, entry := range entries {
			if _, err := marshalReplicationValueBinary(entry); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReplicationStructuredFallbackAppendReuse(b *testing.B) {
	entries := benchmarkStructuredFallbackEntries()
	buffer := make([]byte, 0, 1024)
	b.ReportAllocs()
	b.ResetTimer()
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
