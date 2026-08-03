package hatriecache

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"
)

type commandDumpMapJSONValue struct {
	Label string `json:"label"`
}

func TestCommandDumpMapEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	values := map[string]Map{
		"map:packed-one": {
			"value": "one",
		},
		"map:packed-two": {
			"z-last":  "last",
			"a-first": int64(7),
		},
		"map:regular": {
			"alpha":   "one",
			"bravo":   int64(-2),
			"charlie": uint64(3),
			"delta":   true,
			"echo":    Map{"nested": "map"},
			"foxtrot": Slice{
				"nested",
				int64(4),
			},
			"golf":  []byte("bytes"),
			"hotel": nil,
		},
		"map:empty": {},
		"map:normalized-fallback": {
			"value": commandDumpMapJSONValue{Label: "fallback"},
		},
	}
	for key, value := range values {
		if err := ht.UpsertMapChecked(key, value); err != nil {
			t.Fatalf("UpsertMapChecked(%s) error = %v", key, err)
		}
	}
	if !ht.Expire("map:packed-two", time.Hour) {
		t.Fatal("Expire(map:packed-two) = false")
	}

	for key := range values {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpSmallMapErrorPreservesDestination(t *testing.T) {
	value := smallMapData{
		entries: [smallMapEntryLimit]smallMapEntry{{key: "invalid", value: math.NaN()}},
		length:  1,
	}
	destination := make([]byte, 6, 1024)
	copy(destination, "prefix")

	got, err := appendCommandDumpSmallMapBinary(destination, nil, &value)
	if err == nil {
		t.Fatal("appendCommandDumpSmallMapBinary() error = nil, want unsupported value error")
	}
	if !bytes.Equal(got, []byte("prefix")) {
		t.Fatalf("destination after error = %q, want prefix", got)
	}
	if &got[0] != &destination[0] {
		t.Fatal("appendCommandDumpSmallMapBinary() replaced the destination on error")
	}
}

func BenchmarkCommandDumpPackedMapReuse(b *testing.B) {
	benchmarkCommandDumpMapReuse(b, "map:packed", Map{
		"z-last":  "last",
		"a-first": int64(7),
	})
}

func BenchmarkCommandDumpLargeMapReuse(b *testing.B) {
	value := make(Map, 64)
	for index := 0; index < 64; index++ {
		value[fmt.Sprintf("field-%02d", index)] = int64(index)
	}
	benchmarkCommandDumpMapReuse(b, "map:large", value)
}

func BenchmarkCommandDumpPriorityQueueControlReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	value := make(PriorityQueue, 64)
	for index := range value {
		value[index] = PriorityItem{Priority: int64(index), Value: fmt.Sprintf("value-%02d", index)}
	}
	if err := ht.UpsertPriorityQueueChecked("priority:control", value); err != nil {
		b.Fatalf("UpsertPriorityQueueChecked() error = %v", err)
	}
	benchmarkCommandDumpReuse(b, ht, "priority:control")
}

func benchmarkCommandDumpMapReuse(b *testing.B, key string, value Map) {
	b.Helper()
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertMapChecked(key, value); err != nil {
		b.Fatalf("UpsertMapChecked() error = %v", err)
	}
	benchmarkCommandDumpReuse(b, ht, key)
}

func benchmarkCommandDumpReuse(b *testing.B, ht *HatTrie, key string) {
	b.Helper()
	initial, ok, err := ht.commandDumpEntryBinaryWithoutStats(key)
	if err != nil || !ok {
		b.Fatalf("commandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
	}
	buffer := make([]byte, 0, len(initial))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(initial)), "wire_B/op")
	for iteration := 0; iteration < b.N; iteration++ {
		buffer = buffer[:0]
		buffer, ok, err = ht.appendCommandDumpEntryBinaryWithoutStats(buffer, key)
		if err != nil || !ok {
			b.Fatalf("appendCommandDumpEntryBinaryWithoutStats() = %v/%v", ok, err)
		}
	}
}

func assertCommandDumpMatchesSnapshot(t *testing.T, ht *HatTrie, key string) {
	t.Helper()
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
		t.Fatalf("command dump %s differs from snapshot encoding", key)
	}
	decoded, err := unmarshalReplicationValueBinary(key, got)
	if err != nil {
		t.Fatalf("unmarshalReplicationValueBinary(%s) error = %v", key, err)
	}
	if snapshot.ExpiresAt != nil {
		if decoded.ExpiresAt == nil || !decoded.ExpiresAt.Equal(*snapshot.ExpiresAt) {
			t.Fatalf("decoded command dump %s expiry = %v, want %v", key, decoded.ExpiresAt, snapshot.ExpiresAt)
		}
	}
	if decoded.Key != key || decoded.Type != snapshot.Type {
		t.Fatalf("decoded command dump %s identity = %q/%q, want %q/%q", key, decoded.Key, decoded.Type, key, snapshot.Type)
	}
}
