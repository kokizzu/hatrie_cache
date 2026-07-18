package hatriecache

import (
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
