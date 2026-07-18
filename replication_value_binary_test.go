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
