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

func TestCommandDumpSliceEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	values := map[string]Slice{
		"slice:nil":        nil,
		"slice:empty":      {},
		"slice:packed-one": {"one"},
		"slice:packed-two": {"first", int64(2)},
		"slice:regular": {
			"alpha",
			int64(-2),
			Map{"nested": "map"},
			Slice{"nested", uint64(4)},
		},
		"slice:normalized-fallback": {
			commandDumpMapJSONValue{Label: "fallback"},
		},
		"slice:wrapped": {"shift-a", "shift-b", "keep-a", "keep-b"},
	}
	for key, value := range values {
		if err := ht.UpsertSliceChecked(key, value); err != nil {
			t.Fatalf("UpsertSliceChecked(%s) error = %v", key, err)
		}
	}
	if _, ok, err := ht.ShiftSliceChecked("slice:wrapped"); err != nil || !ok {
		t.Fatalf("ShiftSliceChecked(slice:wrapped) = %v/%v", ok, err)
	}
	if _, ok, err := ht.ShiftSliceChecked("slice:wrapped"); err != nil || !ok {
		t.Fatalf("ShiftSliceChecked(slice:wrapped second) = %v/%v", ok, err)
	}
	if err := ht.PushSliceChecked("slice:wrapped", "wrap-a", "wrap-b"); err != nil {
		t.Fatalf("PushSliceChecked(slice:wrapped) error = %v", err)
	}
	if !ht.Expire("slice:packed-two", time.Hour) {
		t.Fatal("Expire(slice:packed-two) = false")
	}

	for key := range values {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpSetEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	values := map[string]Set{
		"set:empty":         {},
		"set:packed-one":    {"one"},
		"set:packed-two":    {"z-last", "a-first"},
		"set:small-generic": {int64(2), int64(1)},
		"set:regular": {
			"alpha",
			int64(-2),
			Map{"nested": "map"},
			Slice{"nested", uint64(4)},
		},
		"set:normalized-fallback": {
			commandDumpMapJSONValue{Label: "fallback"},
		},
	}
	for key, value := range values {
		if err := ht.UpsertSetChecked(key, value); err != nil {
			t.Fatalf("UpsertSetChecked(%s) error = %v", key, err)
		}
	}
	if !ht.Expire("set:packed-two", time.Hour) {
		t.Fatal("Expire(set:packed-two) = false")
	}

	for key := range values {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpPriorityQueueEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	values := map[string]PriorityQueue{
		"priority:empty": {},
		"priority:ordered": {
			{Priority: 5, Value: "last"},
			{Priority: 1, Value: "first-tie"},
			{Priority: 1, Value: "second-tie"},
			{Priority: 2, Value: ""},
			{Priority: 3, Value: Map{"nested": "map"}},
			{Priority: 4, Value: Slice{"nested", int64(4)}},
		},
		"priority:normalized-fallback": {
			{Priority: 1, Value: commandDumpMapJSONValue{Label: "fallback"}},
		},
	}
	for key, value := range values {
		if err := ht.UpsertPriorityQueueChecked(key, value); err != nil {
			t.Fatalf("UpsertPriorityQueueChecked(%s) error = %v", key, err)
		}
	}
	if !ht.Expire("priority:ordered", time.Hour) {
		t.Fatal("Expire(priority:ordered) = false")
	}

	for key := range values {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpTopKEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	for _, key := range []string{"topk:empty", "topk:ordered", "topk:normalized-fallback"} {
		if err := ht.UpsertTopK(key, 8); err != nil {
			t.Fatalf("UpsertTopK(%s) error = %v", key, err)
		}
	}
	for _, item := range []struct {
		value interface{}
		count uint64
	}{
		{value: "low", count: 1},
		{value: "tie-b", count: 3},
		{value: "tie-a", count: 3},
		{value: Map{"nested": "map"}, count: 2},
		{value: Slice{"nested", int64(4)}, count: 2},
	} {
		if _, err := ht.AddTopKChecked("topk:ordered", item.value, item.count); err != nil {
			t.Fatalf("AddTopKChecked(topk:ordered) error = %v", err)
		}
	}
	if _, err := ht.AddTopKChecked("topk:normalized-fallback", commandDumpMapJSONValue{Label: "fallback"}, 1); err != nil {
		t.Fatalf("AddTopKChecked(topk:normalized-fallback) error = %v", err)
	}
	if !ht.Expire("topk:ordered", time.Hour) {
		t.Fatal("Expire(topk:ordered) = false")
	}

	for _, key := range []string{"topk:empty", "topk:ordered", "topk:normalized-fallback"} {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpReservoirSampleEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	for _, key := range []string{"reservoir:empty", "reservoir:ordered", "reservoir:normalized-fallback"} {
		if err := ht.UpsertReservoirSample(key, 8); err != nil {
			t.Fatalf("UpsertReservoirSample(%s) error = %v", key, err)
		}
	}
	values := []interface{}{
		"first",
		Map{"nested": "map"},
		Slice{"nested", int64(4)},
		"last",
	}
	if _, err := ht.AddReservoirSampleChecked("reservoir:ordered", values[0], values[1:]...); err != nil {
		t.Fatalf("AddReservoirSampleChecked(reservoir:ordered) error = %v", err)
	}
	if _, err := ht.AddReservoirSampleChecked("reservoir:normalized-fallback", commandDumpMapJSONValue{Label: "fallback"}); err != nil {
		t.Fatalf("AddReservoirSampleChecked(reservoir:normalized-fallback) error = %v", err)
	}
	if !ht.Expire("reservoir:ordered", time.Hour) {
		t.Fatal("Expire(reservoir:ordered) = false")
	}

	for _, key := range []string{"reservoir:empty", "reservoir:ordered", "reservoir:normalized-fallback"} {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpRadixTreeEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	for _, key := range []string{"radix:empty", "radix:ordered", "radix:normalized-fallback"} {
		if err := ht.UpsertRadixTreeChecked(key); err != nil {
			t.Fatalf("UpsertRadixTreeChecked(%s) error = %v", key, err)
		}
	}
	values := []struct {
		subkey string
		value  interface{}
	}{
		{subkey: "z-last", value: "last"},
		{subkey: "", value: "root"},
		{subkey: "shared/b", value: Map{"nested": "map"}},
		{subkey: "shared/a", value: Slice{"nested", int64(4)}},
		{subkey: fmt.Sprintf("long-%0300d", 1), value: "spilled-path"},
	}
	for _, item := range values {
		if _, err := ht.PutRadixTreeChecked("radix:ordered", item.subkey, item.value); err != nil {
			t.Fatalf("PutRadixTreeChecked(radix:ordered, %q) error = %v", item.subkey, err)
		}
	}
	if _, err := ht.PutRadixTreeChecked("radix:normalized-fallback", "fallback", commandDumpMapJSONValue{Label: "fallback"}); err != nil {
		t.Fatalf("PutRadixTreeChecked(radix:normalized-fallback) error = %v", err)
	}
	if !ht.Expire("radix:ordered", time.Hour) {
		t.Fatal("Expire(radix:ordered) = false")
	}

	for _, key := range []string{"radix:empty", "radix:ordered", "radix:normalized-fallback"} {
		assertCommandDumpMatchesSnapshot(t, ht, key)
	}
}

func TestCommandDumpStagedXorEncodingMatchesSnapshot(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	for _, key := range []string{"xor:empty", "xor:ordered", "xor:normalized-fallback"} {
		if err := ht.UpsertXorFilter(key, 8); err != nil {
			t.Fatalf("UpsertXorFilter(%s) error = %v", key, err)
		}
	}
	values := []interface{}{
		"z-last",
		"a-first",
		Map{"nested": "map"},
		Slice{"nested", int64(4)},
	}
	if _, err := ht.AddXorFilterChecked("xor:ordered", values[0], values[1:]...); err != nil {
		t.Fatalf("AddXorFilterChecked(xor:ordered) error = %v", err)
	}
	if _, err := ht.AddXorFilterChecked("xor:normalized-fallback", commandDumpMapJSONValue{Label: "fallback"}); err != nil {
		t.Fatalf("AddXorFilterChecked(xor:normalized-fallback) error = %v", err)
	}
	if !ht.Expire("xor:ordered", time.Hour) {
		t.Fatal("Expire(xor:ordered) = false")
	}

	for _, key := range []string{"xor:empty", "xor:ordered", "xor:normalized-fallback"} {
		assertCommandDumpMatchesSnapshot(t, ht, key)
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

func BenchmarkCommandDumpPackedSliceReuse(b *testing.B) {
	benchmarkCommandDumpSliceReuse(b, "slice:packed", Slice{"first", int64(2)})
}

func BenchmarkCommandDumpLargeSliceReuse(b *testing.B) {
	value := make(Slice, 64)
	for index := range value {
		value[index] = int64(index)
	}
	benchmarkCommandDumpSliceReuse(b, "slice:large", value)
}

func BenchmarkCommandDumpPackedSetReuse(b *testing.B) {
	benchmarkCommandDumpSetReuse(b, "set:packed", Set{"z-last", "a-first"})
}

func BenchmarkCommandDumpSmallGenericSetReuse(b *testing.B) {
	benchmarkCommandDumpSetReuse(b, "set:small", Set{int64(2), int64(1)})
}

func BenchmarkCommandDumpLargeSetReuse(b *testing.B) {
	value := make(Set, 64)
	for index := range value {
		value[index] = fmt.Sprintf("value-%02d", index)
	}
	benchmarkCommandDumpSetReuse(b, "set:large", value)
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

func BenchmarkCommandDumpTopKControlReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertTopK("topk:control", 64); err != nil {
		b.Fatalf("UpsertTopK() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if _, err := ht.AddTopKChecked("topk:control", fmt.Sprintf("value-%02d", index), uint64(index+1)); err != nil {
			b.Fatalf("AddTopKChecked() error = %v", err)
		}
	}
	benchmarkCommandDumpReuse(b, ht, "topk:control")
}

func BenchmarkCommandDumpReservoirControlReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertReservoirSample("reservoir:control", 64); err != nil {
		b.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if _, err := ht.AddReservoirSampleChecked("reservoir:control", fmt.Sprintf("value-%02d", index)); err != nil {
			b.Fatalf("AddReservoirSampleChecked() error = %v", err)
		}
	}
	benchmarkCommandDumpReuse(b, ht, "reservoir:control")
}

func BenchmarkCommandDumpRadixTreeControlReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertRadixTreeChecked("radix:control"); err != nil {
		b.Fatalf("UpsertRadixTreeChecked() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if _, err := ht.PutRadixTreeChecked("radix:control", fmt.Sprintf("field-%02d", index), fmt.Sprintf("value-%02d", index)); err != nil {
			b.Fatalf("PutRadixTreeChecked() error = %v", err)
		}
	}
	benchmarkCommandDumpReuse(b, ht, "radix:control")
}

func BenchmarkCommandDumpStagedXorControlReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertXorFilter("xor:control", 64); err != nil {
		b.Fatalf("UpsertXorFilter() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if _, err := ht.AddXorFilterChecked("xor:control", fmt.Sprintf("value-%02d", index)); err != nil {
			b.Fatalf("AddXorFilterChecked() error = %v", err)
		}
	}
	benchmarkCommandDumpReuse(b, ht, "xor:control")
}

func BenchmarkCommandDumpStagedXorFallbackReuse(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertXorFilter("xor:fallback", 64); err != nil {
		b.Fatalf("UpsertXorFilter() error = %v", err)
	}
	for index := 0; index < 63; index++ {
		if _, err := ht.AddXorFilterChecked("xor:fallback", fmt.Sprintf("value-%02d", index)); err != nil {
			b.Fatalf("AddXorFilterChecked() error = %v", err)
		}
	}
	if _, err := ht.AddXorFilterChecked("xor:fallback", commandDumpMapJSONValue{Label: "fallback"}); err != nil {
		b.Fatalf("AddXorFilterChecked(fallback) error = %v", err)
	}
	benchmarkCommandDumpReuse(b, ht, "xor:fallback")
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

func benchmarkCommandDumpSliceReuse(b *testing.B, key string, value Slice) {
	b.Helper()
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertSliceChecked(key, value); err != nil {
		b.Fatalf("UpsertSliceChecked() error = %v", err)
	}
	benchmarkCommandDumpReuse(b, ht, key)
}

func benchmarkCommandDumpSetReuse(b *testing.B, key string, value Set) {
	b.Helper()
	ht := CreateHatTrie()
	defer ht.Destroy()
	if err := ht.UpsertSetChecked(key, value); err != nil {
		b.Fatalf("UpsertSetChecked() error = %v", err)
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
