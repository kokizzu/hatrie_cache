package hatriecache

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestStringSetTransitionSequencePreservesBehavior(t *testing.T) {
	ht := newTestTrie(t)

	if added, err := ht.AddSetChecked("tags", "alpha"); err != nil || added != 1 {
		t.Fatalf("AddSetChecked(alpha) = %d/%v, want 1/nil", added, err)
	}
	if added, err := ht.AddSetChecked("tags", "alpha"); err != nil || added != 0 {
		t.Fatalf("AddSetChecked(duplicate alpha) = %d/%v, want 0/nil", added, err)
	}
	if added, err := ht.AddSetChecked("tags", "beta"); err != nil || added != 1 {
		t.Fatalf("AddSetChecked(beta) = %d/%v, want 1/nil", added, err)
	}
	if !ht.HasSet("tags", "alpha") || !ht.HasSet("tags", "beta") || ht.HasSet("tags", "missing") {
		t.Fatal("two-value set membership mismatch")
	}
	if removed, err := ht.RemoveSetChecked("tags", "alpha"); err != nil || removed != 1 {
		t.Fatalf("RemoveSetChecked(alpha) = %d/%v, want 1/nil", removed, err)
	}
	if added := ht.AddSet("tags", "gamma"); added != 1 {
		t.Fatalf("AddSet(gamma) = %d, want 1", added)
	}
	if added := ht.AddSet("tags", "delta"); added != 1 {
		t.Fatalf("AddSet(delta) = %d, want promotion insertion", added)
	}
	assertSetValueEqual(t, ht, "tags", Set{"beta", "delta", "gamma"})

	nested := Map{"enabled": true}
	if added, err := ht.AddSetChecked("tags", nested); err != nil || added != 1 {
		t.Fatalf("AddSetChecked(nested) = %d/%v, want 1/nil", added, err)
	}
	nested["enabled"] = false
	values := ht.GetSet("tags")
	for _, value := range values {
		if item, ok := value.(Map); ok {
			item["enabled"] = "mutated"
		}
	}
	if !ht.HasSet("tags", Map{"enabled": true}) {
		t.Fatal("stored nested set value changed through caller-owned data")
	}

	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	if !ht.HasSet("tags", "beta") || !ht.HasSet("tags", Map{"enabled": true}) {
		t.Fatal("set membership changed after compaction")
	}

	path := filepath.Join(t.TempDir(), "sets.snapshot")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if !restored.HasSet("tags", "gamma") || !restored.HasSet("tags", Map{"enabled": true}) {
		t.Fatal("set membership changed after snapshot round trip")
	}
}

func TestPackedStringSetStorageTransitionsAndReusesPools(t *testing.T) {
	ht := newTestTrie(t)

	ht.UpsertSet("one", Set{"alpha"})
	oneValue := ht.Get("one")
	oneIndex, twoValuePool, ok := decodePackedStringSetIndex(oneValue.Index)
	if !ok || twoValuePool || oneIndex != 0 {
		t.Fatalf("one-value set index = %d, decoded %d/%v/%v", oneValue.Index, oneIndex, twoValuePool, ok)
	}

	if added := ht.AddSet("one", "beta"); added != 1 {
		t.Fatalf("AddSet(beta) = %d, want 1", added)
	}
	twoValue := ht.Get("one")
	twoIndex, twoValuePool, ok := decodePackedStringSetIndex(twoValue.Index)
	if !ok || !twoValuePool || twoIndex != 0 {
		t.Fatalf("two-value set index = %d, decoded %d/%v/%v", twoValue.Index, twoIndex, twoValuePool, ok)
	}

	ht.UpsertSet("reuse", Set{"gamma"})
	reused := ht.Get("reuse")
	reusedIndex, reusedTwoValuePool, ok := decodePackedStringSetIndex(reused.Index)
	if !ok || reusedTwoValuePool || reusedIndex != oneIndex {
		t.Fatalf("reused one-value index = %d, decoded %d/%v/%v, want pool index %d", reused.Index, reusedIndex, reusedTwoValuePool, ok, oneIndex)
	}

	if added := ht.AddSet("one", "delta"); added != 1 {
		t.Fatalf("AddSet(delta) = %d, want 1", added)
	}
	promoted := ht.Get("one")
	if promoted.Index < 0 {
		t.Fatalf("three-value set index = %d, want generic non-negative index", promoted.Index)
	}
	assertSetValueEqual(t, ht, "one", Set{"alpha", "beta", "delta"})
}

func TestPackedStringSetsSurviveCompactionAndSnapshot(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSet("first", Set{"alpha"})
	ht.UpsertSet("deleted", Set{"discard"})
	ht.UpsertSet("second", Set{"beta", "gamma"})
	ht.Delete("deleted")

	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	assertSetValueEqual(t, ht, "first", Set{"alpha"})
	assertSetValueEqual(t, ht, "second", Set{"beta", "gamma"})

	path := filepath.Join(t.TempDir(), "packed-sets.snapshot")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	for _, key := range []string{"first", "second"} {
		if index := restored.Get(key).Index; index >= 0 {
			t.Fatalf("restored %s index = %d, want packed negative index", key, index)
		}
	}
	assertSetValueEqual(t, restored, "first", Set{"alpha"})
	assertSetValueEqual(t, restored, "second", Set{"beta", "gamma"})
}

func TestStringSetPromotionPreservesEscapedValues(t *testing.T) {
	ht := newTestTrie(t)
	values := []string{"line\nfeed", `quote"value`, `slash\\value`, "plain"}
	for _, value := range values {
		if added := ht.AddSet("escaped", value); added != 1 {
			t.Fatalf("AddSet(%q) = %d, want 1", value, added)
		}
	}
	for _, value := range values {
		if !ht.HasSet("escaped", value) {
			t.Fatalf("HasSet(%q) = false after promotion", value)
		}
	}
	if removed := ht.RemoveSet("escaped", values[0], values[1], values[2], values[3]); removed != len(values) {
		t.Fatalf("RemoveSet(escaped values) = %d, want %d", removed, len(values))
	}
}

func TestPackedStringSetPreservesEmptyStringAcrossEmptyTransition(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSet("zero", Set{})
	zeroIndex, zeroTwoValuePool, zeroPacked := decodePackedStringSetIndex(ht.Get("zero").Index)
	if !zeroPacked || zeroTwoValuePool || zeroIndex != 0 {
		t.Fatalf("empty set did not use one-value packed pool: %d/%v/%v", zeroIndex, zeroTwoValuePool, zeroPacked)
	}
	assertSetValueEqual(t, ht, "zero", Set{})

	ht.UpsertSet("empty", Set{""})
	if !ht.HasSet("empty", "") {
		t.Fatal("HasSet(empty string) = false")
	}
	if removed := ht.RemoveSet("empty", ""); removed != 1 {
		t.Fatalf("RemoveSet(empty string) = %d, want 1", removed)
	}
	assertSetValueEqual(t, ht, "empty", Set{})
	if added := ht.AddSet("empty", ""); added != 1 {
		t.Fatalf("AddSet(empty string) = %d, want 1", added)
	}
	assertSetValueEqual(t, ht, "empty", Set{""})
}

func assertSetValueEqual(t *testing.T, ht *HatTrie, key string, want Set) {
	t.Helper()
	got, ok, err := ht.GetSetChecked(key)
	if err != nil || !ok {
		t.Fatalf("GetSetChecked(%q) = %#v/%v/%v", key, got, ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSetChecked(%q) = %#v, want %#v", key, got, want)
	}
}

func BenchmarkSetStorageLayout100k(b *testing.B) {
	const keyCount = 100000
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = fmt.Sprintf("set-layout:%09d", index)
	}

	for _, valueCount := range []int{1, 2, 3, 8, 16} {
		valueCount := valueCount
		values := make(Set, valueCount)
		for value := 0; value < valueCount; value++ {
			values[value] = fmt.Sprintf("value:%02d", value)
		}
		b.Run(fmt.Sprintf("Values%d", valueCount), func(b *testing.B) {
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
					ht.UpsertSet(key, values)
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
				if got := ht.GetSet(keys[keyCount-1]); len(got) != valueCount {
					ht.Destroy()
					b.Fatalf("last set values = %d, want %d", len(got), valueCount)
				}
				runtime.KeepAlive(ht)
				ht.Destroy()
				b.StartTimer()
			}
			b.StopTimer()
			operations := float64(b.N * keyCount)
			b.ReportMetric(float64(elapsed.Nanoseconds())/operations, "ns/set")
			b.ReportMetric(float64(retainedBytes)/operations, "retained_B/set")
			b.ReportMetric(float64(retainedObjects)/operations, "retained_objects/set")
		})
	}
}

func BenchmarkSetStorageOperations(b *testing.B) {
	b.Run("SmallDuplicateAdd", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.AddSet("set", "alpha")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if added := ht.AddSet("set", "alpha"); added != 0 {
				b.Fatalf("AddSet(duplicate) = %d, want 0", added)
			}
		}
	})

	b.Run("SmallHas", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSet("set", Set{"alpha", "beta"})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if !ht.HasSet("set", "beta") {
				b.Fatal("HasSet(beta) = false")
			}
		}
	})

	b.Run("SmallRemoveAdd", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.AddSet("set", "alpha")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if removed := ht.RemoveSet("set", "alpha"); removed != 1 {
				b.Fatalf("RemoveSet(alpha) = %d, want 1", removed)
			}
			if added := ht.AddSet("set", "alpha"); added != 1 {
				b.Fatalf("AddSet(alpha) = %d, want 1", added)
			}
		}
	})

	b.Run("PromoteThird", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		values := Set{"alpha", "beta"}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.UpsertSet("set", values)
			if added := ht.AddSet("set", "gamma"); added != 1 {
				b.Fatalf("AddSet(gamma) = %d, want 1", added)
			}
		}
	})

	b.Run("LargeHas", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		values := make(Set, 8)
		for index := range values {
			values[index] = fmt.Sprintf("value:%02d", index)
		}
		ht.UpsertSet("set", values)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if !ht.HasSet("set", "value:07") {
				b.Fatal("HasSet(value:07) = false")
			}
		}
	})

	b.Run("SmallValues", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSet("set", Set{"alpha", "beta"})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if values := ht.GetSet("set"); len(values) != 2 {
				b.Fatalf("GetSet() length = %d, want 2", len(values))
			}
		}
	})
}
