package hatriecache

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestAdaptiveSmallMapPromotionAndReplacement(t *testing.T) {
	ht := newTestTrie(t)
	ht.PutMap("profile", "first", int64(1))
	packed := ht.Get("profile")
	if _, ok := decodeSmallMapIndex(packed.Index); !ok {
		t.Fatalf("one-field map index = %d, want packed index", packed.Index)
	}

	ht.PutMap("profile", "second", int64(2))
	twoFields := ht.Get("profile")
	if twoFields.Index != packed.Index {
		t.Fatalf("two-field map index = %d, want retained packed index %d", twoFields.Index, packed.Index)
	}

	ht.PutMap("profile", "third", int64(3))
	promoted := ht.Get("profile")
	if promoted.Index < 0 {
		t.Fatalf("three-field map index = %d, want promoted Go map index", promoted.Index)
	}
	if !mapIndexReleased(ht, packed.Index) {
		t.Fatalf("packed map index %d was not released after promotion", packed.Index)
	}

	ht.UpsertMap("profile", Map{"only": int64(9)})
	repacked := ht.Get("profile")
	if _, ok := decodeSmallMapIndex(repacked.Index); !ok {
		t.Fatalf("replacement one-field map index = %d, want packed index", repacked.Index)
	}
	if !mapIndexReleased(ht, promoted.Index) {
		t.Fatalf("promoted map index %d was not released after replacement", promoted.Index)
	}
	assertMapValueEqual(t, ht, "profile", Map{"only": int64(9)})
}

func TestAdaptiveSmallMapCompactionAndSnapshotRoundTrip(t *testing.T) {
	ht := newTestTrie(t)
	ht.PutMap("first", "field", Map{"nested": true})
	ht.PutMap("deleted", "field", "remove")
	ht.PutMap("last", "field", []interface{}{"a", "b"})
	ht.Delete("deleted")

	lastBefore := ht.Get("last")
	if _, ok := decodeSmallMapIndex(lastBefore.Index); !ok {
		t.Fatalf("last map index before compaction = %d, want packed index", lastBefore.Index)
	}
	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	lastAfter := ht.Get("last")
	if _, ok := decodeSmallMapIndex(lastAfter.Index); !ok {
		t.Fatalf("last map index after compaction = %d, want packed index", lastAfter.Index)
	}
	assertMapValueEqual(t, ht, "first", Map{"field": Map{"nested": true}})
	assertMapValueEqual(t, ht, "last", Map{"field": []interface{}{"a", "b"}})

	path := filepath.Join(t.TempDir(), "small-maps.snapshot")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if hval := restored.Get("first"); hval.Index >= 0 || !hval.IsMap() {
		t.Fatalf("restored first map = %v, want packed map", hval)
	}
	assertMapValueEqual(t, restored, "first", Map{"field": Map{"nested": true}})
	assertMapValueEqual(t, restored, "last", Map{"field": []interface{}{"a", "b"}})
}

func TestSmallMapJSONStringMatchesGenericEncoding(t *testing.T) {
	tests := []Map{
		{"quote\"\\\n<>&\u2028": "value\"\\\t<>&\u2029"},
		{"second": int64(-42), "first": true},
		{"nested": Map{"items": []interface{}{"a", float64(1.5)}}},
		{"nothing": nil, "unsigned": uint64(18446744073709551615)},
	}
	for _, value := range tests {
		data := newSmallMapData(value)
		got, err := data.jsonString()
		if err != nil {
			t.Fatalf("smallMapData.jsonString(%#v) error = %v", value, err)
		}
		want, err := jsonEncodedString(value)
		if err != nil {
			t.Fatalf("jsonEncodedString(%#v) error = %v", value, err)
		}
		if got != want {
			t.Fatalf("smallMapData.jsonString(%#v) = %q, want %q", value, got, want)
		}
	}
}

func TestMapSmallToLargeMutationSequencePreservesBehavior(t *testing.T) {
	ht := newTestTrie(t)
	nested := Map{"enabled": true}

	if err := ht.PutMapChecked("profile", "first", nested); err != nil {
		t.Fatalf("PutMapChecked(first) error = %v", err)
	}
	nested["enabled"] = false
	if got, ok, err := ht.PeekMapChecked("profile", "first"); err != nil || !ok {
		t.Fatalf("PeekMapChecked(first) = %#v/%v/%v", got, ok, err)
	} else if value := got.(Map)["enabled"]; value != true {
		t.Fatalf("PeekMapChecked(first).enabled = %#v, want true", value)
	}

	if err := ht.PutMapEntriesChecked("profile", Map{
		"second": "two",
		"third":  int64(3),
	}); err != nil {
		t.Fatalf("PutMapEntriesChecked(second/third) error = %v", err)
	}
	if err := ht.PutMapChecked("profile", "first", Map{"enabled": "updated"}); err != nil {
		t.Fatalf("PutMapChecked(update first) error = %v", err)
	}
	if got, ok, err := ht.TakeMapChecked("profile", "second"); err != nil || !ok || got != "two" {
		t.Fatalf("TakeMapChecked(second) = %#v/%v/%v, want two/true/nil", got, ok, err)
	}
	if err := ht.PutMapChecked("profile", "fourth", []interface{}{"a", "b"}); err != nil {
		t.Fatalf("PutMapChecked(fourth) error = %v", err)
	}

	want := Map{
		"first":  Map{"enabled": "updated"},
		"third":  int64(3),
		"fourth": []interface{}{"a", "b"},
	}
	assertMapValueEqual(t, ht, "profile", want)

	got := ht.GetMap("profile")
	got["third"] = int64(99)
	got["first"].(Map)["enabled"] = "mutated"
	got["fourth"].([]interface{})[0] = "mutated"
	assertMapValueEqual(t, ht, "profile", want)

	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	assertMapValueEqual(t, ht, "profile", want)

	if got, ok, err := ht.TakeMapChecked("profile", "missing"); err != nil || ok || got != nil {
		t.Fatalf("TakeMapChecked(missing) = %#v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got := ht.Get("profile"); !got.IsMap() {
		t.Fatalf("Get(profile) = %v, want map", got)
	}
}

func assertMapValueEqual(t *testing.T, ht *HatTrie, key string, want Map) {
	t.Helper()
	got, ok, err := ht.GetMapChecked(key)
	if err != nil || !ok {
		t.Fatalf("GetMapChecked(%q) = %#v/%v/%v", key, got, ok, err)
	}
	if gotJSON, wantJSON := mustMapJSON(t, got), mustMapJSON(t, want); gotJSON != wantJSON {
		t.Fatalf("GetMapChecked(%q) = %s, want %s", key, gotJSON, wantJSON)
	}
}

func mustMapJSON(t *testing.T, value Map) string {
	t.Helper()
	encoded, err := MarshalMapJSON(value)
	if err != nil {
		t.Fatalf("MarshalMapJSON(%#v) error = %v", value, err)
	}
	return string(encoded)
}

func BenchmarkMapStorageLayout100k(b *testing.B) {
	const keyCount = 100000
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = fmt.Sprintf("map-layout:%09d", index)
	}

	for _, fieldCount := range []int{1, 2, 4, 8, 16} {
		fieldCount := fieldCount
		fields := make(Map, fieldCount)
		for field := 0; field < fieldCount; field++ {
			fields[fmt.Sprintf("field:%02d", field)] = int64(field)
		}
		b.Run(fmt.Sprintf("Fields%d", fieldCount), func(b *testing.B) {
			var retainedBytes uint64
			var retainedObjects uint64
			var elapsed time.Duration
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				ht := CreateHatTrie()
				b.StartTimer()
				started := time.Now()
				for _, key := range keys {
					ht.UpsertMap(key, fields)
				}
				elapsed += time.Since(started)
				b.StopTimer()
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > before.HeapAlloc {
					retainedBytes += after.HeapAlloc - before.HeapAlloc
				}
				if after.HeapObjects > before.HeapObjects {
					retainedObjects += after.HeapObjects - before.HeapObjects
				}
				if got := ht.GetMap(keys[keyCount-1]); len(got) != fieldCount {
					ht.Destroy()
					b.Fatalf("last map fields = %d, want %d", len(got), fieldCount)
				}
				runtime.KeepAlive(ht)
				ht.Destroy()
				b.StartTimer()
			}
			b.StopTimer()
			operations := float64(b.N * keyCount)
			b.ReportMetric(float64(elapsed.Nanoseconds())/operations, "ns/map")
			b.ReportMetric(float64(retainedBytes)/operations, "retained_B/map")
			b.ReportMetric(float64(retainedObjects)/operations, "retained_objects/map")
		})
	}
}

func BenchmarkMapStorageOperations(b *testing.B) {
	b.Run("SmallPutExisting", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.PutMap("map", "field", int64(0))
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.PutMap("map", "field", int64(index))
		}
	})

	b.Run("SmallPeek", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.PutMap("map", "field", int64(42))
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if value, ok := ht.PeekMap("map", "field").(int64); !ok || value != 42 {
				b.Fatalf("PeekMap() = %#v, want 42", value)
			}
		}
	})

	b.Run("SmallTakePut", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.PutMap("map", "field", int64(42))
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if value := ht.TakeMap("map", "field"); value != int64(42) {
				b.Fatalf("TakeMap() = %#v, want 42", value)
			}
			ht.PutMap("map", "field", int64(42))
		}
	})

	b.Run("PromoteThird", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		fields := Map{"first": int64(1), "second": int64(2)}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.UpsertMap("map", fields)
			ht.PutMap("map", "third", int64(3))
		}
	})

	b.Run("LargePutExisting", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		fields := make(Map, 8)
		for field := 0; field < 8; field++ {
			fields[fmt.Sprintf("field:%02d", field)] = int64(field)
		}
		ht.UpsertMap("map", fields)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.PutMap("map", "field:00", int64(index))
		}
	})

	b.Run("LargePeek", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		fields := make(Map, 8)
		for field := 0; field < 8; field++ {
			fields[fmt.Sprintf("field:%02d", field)] = int64(field)
		}
		ht.UpsertMap("map", fields)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if value, ok := ht.PeekMap("map", "field:07").(int64); !ok || value != 7 {
				b.Fatalf("PeekMap() = %#v, want 7", value)
			}
		}
	})
}
