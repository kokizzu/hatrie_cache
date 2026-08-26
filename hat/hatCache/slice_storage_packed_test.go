package hatCache

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"hatrie_cache/hat/hatCodec"
)

func TestSliceTransitionSequencePreservesBehavior(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("queue", Slice{})
	assertSliceValueEqual(t, ht, "queue", Slice{})

	if err := ht.PushSliceChecked("queue", nil); err != nil {
		t.Fatalf("PushSliceChecked(nil) error = %v", err)
	}
	if head, ok, err := ht.HeadSliceChecked("queue"); err != nil || !ok || head != nil {
		t.Fatalf("HeadSliceChecked() = %#v/%v/%v, want nil/true/nil", head, ok, err)
	}

	payload := Map{"state": "stored"}
	if err := ht.PushSliceChecked("queue", payload); err != nil {
		t.Fatalf("PushSliceChecked(payload) error = %v", err)
	}
	payload["state"] = "caller"
	assertSliceValueEqual(t, ht, "queue", Slice{nil, Map{"state": "stored"}})

	shifted, ok, err := ht.ShiftSliceChecked("queue")
	if err != nil || !ok || shifted != nil {
		t.Fatalf("ShiftSliceChecked() = %#v/%v/%v, want nil/true/nil", shifted, ok, err)
	}
	head := ht.HeadSlice("queue").(Map)
	head["state"] = "reader"
	if got := ht.HeadSlice("queue").(Map)["state"]; got != "stored" {
		t.Fatalf("HeadSlice() exposed nested value, got state %#v", got)
	}

	if err := ht.PushSliceChecked("queue", "middle", "tail"); err != nil {
		t.Fatalf("PushSliceChecked(middle, tail) error = %v", err)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{Map{"state": "stored"}, "middle", "tail"})
	if popped := ht.PopSlice("queue"); popped != "tail" {
		t.Fatalf("PopSlice() = %#v, want tail", popped)
	}
	if tail := ht.TailSlice("queue"); tail != "middle" {
		t.Fatalf("TailSlice() = %#v, want middle", tail)
	}

	ht.UpsertSlice("tiny", Slice{nil, "small"})
	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{Map{"state": "stored"}, "middle"})
	assertSliceValueEqual(t, ht, "tiny", Slice{nil, "small"})

	path := filepath.Join(t.TempDir(), "slices.snapshot")
	if err := ht.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	assertSliceValueEqual(t, restored, "queue", Slice{Map{"state": "stored"}, "middle"})
	assertSliceValueEqual(t, restored, "tiny", Slice{nil, "small"})
}

func TestPackedSliceStorageTransitionsReuseAndClearSlots(t *testing.T) {
	ht := newTestTrie(t)
	payload := &struct{ name string }{name: "first"}
	ht.PushSlice("queue", payload)
	oneValue := ht.Get("queue")
	oneIndex, twoValuePool, ok := decodePackedSliceIndex(oneValue.Index)
	if !ok || twoValuePool || oneIndex != 0 {
		t.Fatalf("one-value slice index = %d, decoded %d/%v/%v", oneValue.Index, oneIndex, twoValuePool, ok)
	}

	ht.PushSlice("queue", "second")
	twoValue := ht.Get("queue")
	twoIndex, twoValuePool, ok := decodePackedSliceIndex(twoValue.Index)
	if !ok || !twoValuePool || twoIndex != 0 {
		t.Fatalf("two-value slice index = %d, decoded %d/%v/%v", twoValue.Index, twoIndex, twoValuePool, ok)
	}
	if !sliceIndexReleased(ht, oneValue.Index) {
		t.Fatalf("one-value backing index %d was not released after transition", oneValue.Index)
	}

	ht.UpsertSlice("reuse", Slice{"reused"})
	reusedIndex, reusedTwoValuePool, ok := decodePackedSliceIndex(ht.Get("reuse").Index)
	if !ok || reusedTwoValuePool || reusedIndex != oneIndex {
		t.Fatalf("reused one-value index = %d/%v/%v, want %d/false/true", reusedIndex, reusedTwoValuePool, ok, oneIndex)
	}

	if shifted := ht.ShiftSlice("queue"); shifted != payload {
		t.Fatalf("ShiftSlice(first) = %#v, want payload", shifted)
	}
	if shifted := ht.ShiftSlice("queue"); shifted != "second" {
		t.Fatalf("ShiftSlice(second) = %#v, want second", shifted)
	}
	data := ht.slices.twoValues[twoIndex]
	if data.length != 0 || data.values[0] != nil || data.values[1] != nil {
		t.Fatalf("drained packed slice retained slots: %#v", data)
	}

	ht.PushSlice("queue", "third", "fourth", "fifth")
	promoted := ht.Get("queue")
	if promoted.Index < 0 {
		t.Fatalf("three-value slice index = %d, want generic non-negative index", promoted.Index)
	}
	if !sliceIndexReleased(ht, twoValue.Index) {
		t.Fatalf("two-value backing index %d was not released after promotion", twoValue.Index)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{"third", "fourth", "fifth"})
}

func TestPackedTwoSliceStateTransitionsPreserveNilValues(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("queue", Slice{"first", nil})

	index := ht.Get("queue").Index
	poolIndex, twoValuePool, packed := decodePackedSliceIndex(index)
	if !packed || !twoValuePool {
		t.Fatalf("two-value slice index = %d/%v/%v, want packed two-value pool", poolIndex, twoValuePool, packed)
	}
	if got, ok := ht.slices.length(index); !ok || got != 2 {
		t.Fatalf("initial length = %d/%v, want 2/true", got, ok)
	}
	if value, ok, err := ht.PopSliceChecked("queue"); err != nil || !ok || value != nil {
		t.Fatalf("PopSliceChecked(nil) = %#v/%v/%v, want nil/true/nil", value, ok, err)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{"first"})
	if got, ok := ht.slices.length(index); !ok || got != 1 {
		t.Fatalf("length after pop = %d/%v, want 1/true", got, ok)
	}

	if err := ht.PushSliceChecked("queue", nil); err != nil {
		t.Fatalf("PushSliceChecked(nil) error = %v", err)
	}
	if got := ht.Get("queue").Index; got != index {
		t.Fatalf("two-value pool index after refill = %d, want %d", got, index)
	}
	if value, ok, err := ht.ShiftSliceChecked("queue"); err != nil || !ok || value != "first" {
		t.Fatalf("ShiftSliceChecked(first) = %#v/%v/%v, want first/true/nil", value, ok, err)
	}
	if head, ok, err := ht.HeadSliceChecked("queue"); err != nil || !ok || head != nil {
		t.Fatalf("HeadSliceChecked(nil) = %#v/%v/%v, want nil/true/nil", head, ok, err)
	}
	if tail, ok, err := ht.TailSliceChecked("queue"); err != nil || !ok || tail != nil {
		t.Fatalf("TailSliceChecked(nil) = %#v/%v/%v, want nil/true/nil", tail, ok, err)
	}
	if value, ok, err := ht.ShiftSliceChecked("queue"); err != nil || !ok || value != nil {
		t.Fatalf("ShiftSliceChecked(nil) = %#v/%v/%v, want nil/true/nil", value, ok, err)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{})
	if got, ok := ht.slices.length(index); !ok || got != 0 {
		t.Fatalf("drained length = %d/%v, want 0/true", got, ok)
	}
	if value, ok, err := ht.PopSliceChecked("queue"); err != nil || ok || value != nil {
		t.Fatalf("PopSliceChecked(empty) = %#v/%v/%v, want nil/false/nil", value, ok, err)
	}

	if err := ht.PushSliceChecked("queue", nil, "last"); err != nil {
		t.Fatalf("PushSliceChecked(nil, last) error = %v", err)
	}
	if got := ht.Get("queue").Index; got != index {
		t.Fatalf("drained pool index after refill = %d, want %d", got, index)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{nil, "last"})
}

func TestPackedTwoSliceLayoutIsBounded(t *testing.T) {
	if got := unsafe.Sizeof(packedTwoSlice{}); got != 40 {
		t.Fatalf("packedTwoSlice size = %d, want 40 bytes", got)
	}
}

func TestPackedSlicesPreserveNilAndEmptyContainers(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("nil", nil)
	ht.UpsertSlice("empty", Slice{})

	nilValue, ok, err := ht.GetSliceChecked("nil")
	if err != nil || !ok || nilValue != nil {
		t.Fatalf("GetSliceChecked(nil) = %#v/%v/%v, want nil/true/nil", nilValue, ok, err)
	}
	emptyValue, ok, err := ht.GetSliceChecked("empty")
	if err != nil || !ok || emptyValue == nil || len(emptyValue) != 0 {
		t.Fatalf("GetSliceChecked(empty) = %#v/%v/%v, want non-nil empty/true/nil", emptyValue, ok, err)
	}
	for _, key := range []string{"nil", "empty"} {
		if _, twoValuePool, packed := decodePackedSliceIndex(ht.Get(key).Index); !packed || twoValuePool {
			t.Fatalf("%s did not use packed one-value pool", key)
		}
	}
}

func TestPackedSlicesSurviveCompactionAndSnapshot(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("first", Slice{"alpha"})
	ht.UpsertSlice("deleted", Slice{"discard"})
	ht.UpsertSlice("second", Slice{nil, "beta"})
	ht.UpsertSlice("nil", nil)
	ht.UpsertSlice("empty", Slice{})
	ht.Delete("deleted")

	if _, err := ht.CompactMemory(); err != nil {
		t.Fatalf("CompactMemory() error = %v", err)
	}
	assertSliceValueEqual(t, ht, "first", Slice{"alpha"})
	assertSliceValueEqual(t, ht, "second", Slice{nil, "beta"})
	assertSliceValueEqual(t, ht, "nil", nil)
	assertSliceValueEqual(t, ht, "empty", Slice{})

	path := filepath.Join(t.TempDir(), "packed-slices.snapshot")
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
	assertSliceValueEqual(t, restored, "first", Slice{"alpha"})
	assertSliceValueEqual(t, restored, "second", Slice{nil, "beta"})
}

func TestPackedSliceExactCommandTransitionsUpdateBackingIndex(t *testing.T) {
	ht := newTestTrie(t)
	push := func(value string) {
		t.Helper()
		response := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHSLICE", Key: "queue", Value: value})
		if !response.OK {
			t.Fatalf("PUSHSLICE(%q) response = %#v", value, response)
		}
	}

	push("first")
	one := ht.Get("queue")
	if _, twoValuePool, packed := decodePackedSliceIndex(one.Index); !packed || twoValuePool {
		t.Fatalf("one-value command index = %d, want packed one-value pool", one.Index)
	}
	push("second")
	two := ht.Get("queue")
	if _, twoValuePool, packed := decodePackedSliceIndex(two.Index); !packed || !twoValuePool {
		t.Fatalf("two-value command index = %d, want packed two-value pool", two.Index)
	}
	push("third")
	three := ht.Get("queue")
	if three.Index < 0 {
		t.Fatalf("three-value command index = %d, want generic deque", three.Index)
	}
	assertSliceValueEqual(t, ht, "queue", Slice{"first", "second", "third"})
}

func assertSliceValueEqual(t *testing.T, ht *HatTrie, key string, want Slice) {
	t.Helper()
	got, ok, err := ht.GetSliceChecked(key)
	if err != nil || !ok {
		t.Fatalf("GetSliceChecked(%q) = %#v/%v/%v", key, got, ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSliceChecked(%q) = %#v, want %#v", key, got, want)
	}
}

func TestPackedSliceCommandGetMatchesCanonicalJSON(t *testing.T) {
	for _, test := range []struct {
		name   string
		values Slice
	}{
		{name: "nil", values: nil},
		{name: "empty", values: Slice{}},
		{name: "one nil", values: Slice{nil}},
		{name: "one escaped", values: Slice{"<alpha>\n\"quoted\""}},
		{name: "one invalid utf8", values: Slice{string([]byte{'a', 0xff, 'b'})}},
		{name: "two unicode separators", values: Slice{"line\u2028separator", "paragraph\u2029separator"}},
		{name: "two scalar", values: Slice{true, int64(-9223372036854775807)}},
		{name: "two nested", values: Slice{Map{"<key>": "value"}, Slice{1, "two"}}},
		{name: "promoted", values: Slice{"alpha", "beta", "gamma"}},
		{name: "promoted mixed", values: Slice{"<alpha>\n", nil, Map{"nested": Slice{true, int64(-7)}}, uint64(^uint64(0))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ht := newTestTrie(t)
			ht.UpsertSlice("slice", test.values)

			want, err := hatCodec.JSONEncodedString(ht.GetSlice("slice"))
			if err != nil {
				t.Fatalf("jsonEncodedString() error = %v", err)
			}
			got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"})
			if !got.OK || got.Value != want {
				t.Fatalf("GET response = %#v, want value %q", got, want)
			}
		})
	}

	t.Run("wrapped promoted", func(t *testing.T) {
		ht := newTestTrie(t)
		ht.UpsertSlice("slice", Slice{"alpha", "beta", "gamma", "delta"})
		for range 2 {
			if _, ok, err := ht.ShiftSliceChecked("slice"); err != nil || !ok {
				t.Fatalf("ShiftSliceChecked() = %v/%v, want value/nil", ok, err)
			}
		}
		if err := ht.PushSliceChecked("slice", "epsilon", "zeta"); err != nil {
			t.Fatalf("PushSliceChecked() error = %v", err)
		}
		want := `["gamma","delta","epsilon","zeta"]`
		got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"})
		if !got.OK || got.Value != want {
			t.Fatalf("GET wrapped response = %#v, want value %q", got, want)
		}
	})

	t.Run("promoted emptied", func(t *testing.T) {
		ht := newTestTrie(t)
		ht.UpsertSlice("slice", Slice{"alpha", "beta", "gamma"})
		for range 3 {
			if _, ok, err := ht.PopSliceChecked("slice"); err != nil || !ok {
				t.Fatalf("PopSliceChecked() = %v/%v, want value/nil", ok, err)
			}
		}
		got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"})
		if !got.OK || got.Value != "[]" {
			t.Fatalf("GET emptied response = %#v, want []", got)
		}
	})

	t.Run("promoted marshal error", func(t *testing.T) {
		ht := newTestTrie(t)
		value := &mutableSliceJSONValue{}
		if err := ht.UpsertSliceChecked("slice", Slice{"alpha", "beta", value}); err != nil {
			t.Fatalf("UpsertSliceChecked() error = %v", err)
		}
		value.fail = true
		if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"}); got.OK {
			t.Fatalf("GET response = %#v, want marshal error", got)
		}
	})
}

type mutableSliceJSONValue struct {
	fail bool
}

func (value *mutableSliceJSONValue) MarshalJSON() ([]byte, error) {
	if value.fail {
		return nil, fmt.Errorf("forced slice JSON error")
	}
	return []byte(`{"ok":true}`), nil
}

func BenchmarkSliceStorageLayout100k(b *testing.B) {
	const keyCount = 100000
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = fmt.Sprintf("slice-layout:%09d", index)
	}

	type layoutCase struct {
		name       string
		valueCount int
		push       bool
		promote    bool
	}
	cases := []layoutCase{
		{name: "UpsertValues0", valueCount: 0},
		{name: "UpsertValues1", valueCount: 1},
		{name: "UpsertValues2", valueCount: 2},
		{name: "UpsertValues3", valueCount: 3},
		{name: "UpsertValues8", valueCount: 8},
		{name: "UpsertValues16", valueCount: 16},
		{name: "PushValues1", valueCount: 1, push: true},
		{name: "PushValues2", valueCount: 2, push: true},
		{name: "PromoteValues2To3", valueCount: 2, promote: true},
	}
	for _, benchmark := range cases {
		benchmark := benchmark
		values := make(Slice, benchmark.valueCount)
		for value := range values {
			values[value] = fmt.Sprintf("value:%02d", value)
		}
		b.Run(benchmark.name, func(b *testing.B) {
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
					if benchmark.promote {
						ht.UpsertSlice(key, values)
						ht.PushSlice(key, "value:02")
					} else if benchmark.push {
						ht.PushSlice(key, values[0], values[1:]...)
					} else {
						ht.UpsertSlice(key, values)
					}
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
				wantCount := benchmark.valueCount
				if benchmark.promote {
					wantCount++
				}
				if got := ht.GetSlice(keys[keyCount-1]); len(got) != wantCount {
					ht.Destroy()
					b.Fatalf("last slice values = %d, want %d", len(got), wantCount)
				}
				runtime.KeepAlive(ht)
				ht.Destroy()
				b.StartTimer()
			}
			b.StopTimer()
			operations := float64(b.N * keyCount)
			b.ReportMetric(float64(elapsed.Nanoseconds())/operations, "ns/slice")
			b.ReportMetric(float64(retainedBytes)/operations, "retained_B/slice")
			b.ReportMetric(float64(retainedObjects)/operations, "retained_objects/slice")
		})
	}
}

func BenchmarkSliceStorageOperations(b *testing.B) {
	b.Run("SmallPushPop", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSlice("slice", Slice{})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.PushSlice("slice", "value")
			if value := ht.PopSlice("slice"); value != "value" {
				b.Fatalf("PopSlice() = %#v, want value", value)
			}
		}
	})

	b.Run("SmallPushShift", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSlice("slice", Slice{})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.PushSlice("slice", "value")
			if value := ht.ShiftSlice("slice"); value != "value" {
				b.Fatalf("ShiftSlice() = %#v, want value", value)
			}
		}
	})

	b.Run("SmallHeadTail", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSlice("slice", Slice{"first", "last"})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if head, tail := ht.HeadSlice("slice"), ht.TailSlice("slice"); head != "first" || tail != "last" {
				b.Fatalf("HeadSlice/TailSlice = %#v/%#v", head, tail)
			}
		}
	})

	b.Run("PromoteThird", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		values := Slice{"first", "second"}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.UpsertSlice("slice", values)
			ht.PushSlice("slice", "third")
		}
	})

	b.Run("LargePushPop", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		values := make(Slice, 8)
		for index := range values {
			values[index] = fmt.Sprintf("value:%02d", index)
		}
		ht.UpsertSlice("slice", values)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			ht.PushSlice("slice", "last")
			if value := ht.PopSlice("slice"); value != "last" {
				b.Fatalf("PopSlice() = %#v, want last", value)
			}
		}
	})

	b.Run("SmallValues", func(b *testing.B) {
		ht := CreateHatTrie()
		b.Cleanup(ht.Destroy)
		ht.UpsertSlice("slice", Slice{"first", "last"})
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if values := ht.GetSlice("slice"); len(values) != 2 {
				b.Fatalf("GetSlice() length = %d, want 2", len(values))
			}
		}
	})
}

func BenchmarkSlicePackedCommandChurn(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		setup func(*HatTrie)
	}{
		{
			name: "GenericDeque",
			setup: func(ht *HatTrie) {
				ht.UpsertSlice("slice", Slice{"first", "second", "third"})
				for index := 0; index < 3; index++ {
					ht.ExecuteCommand(CacheCommandRequest{Command: "POPSLICE", Key: "slice"})
				}
			},
		},
		{
			name: "Packed",
			setup: func(ht *HatTrie) {
				ht.UpsertSlice("slice", Slice{})
			},
		},
	} {
		benchmark := benchmark
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			benchmark.setup(ht)
			push := CacheCommandRequest{Command: "PUSHSLICE", Key: "slice", Value: "value"}
			pop := CacheCommandRequest{Command: "POPSLICE", Key: "slice"}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if response := ht.ExecuteCommand(push); !response.OK {
					b.Fatalf("PUSHSLICE response = %#v", response)
				}
				if response := ht.ExecuteCommand(pop); !response.OK || response.Value != "value" {
					b.Fatalf("POPSLICE response = %#v", response)
				}
			}
		})
	}
}

func BenchmarkSlicePackedOperationPairs(b *testing.B) {
	setups := []struct {
		name  string
		setup func(*HatTrie)
	}{
		{
			name: "GenericDeque",
			setup: func(ht *HatTrie) {
				ht.UpsertSlice("slice", Slice{"first", "last", "discard"})
				ht.PopSlice("slice")
			},
		},
		{
			name: "Packed",
			setup: func(ht *HatTrie) {
				ht.UpsertSlice("slice", Slice{"first", "last"})
			},
		},
	}
	for _, benchmark := range setups {
		benchmark := benchmark
		b.Run("HeadTail/"+benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			benchmark.setup(ht)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if head, tail := ht.HeadSlice("slice"), ht.TailSlice("slice"); head != "first" || tail != "last" {
					b.Fatalf("HeadSlice/TailSlice = %#v/%#v", head, tail)
				}
			}
		})
		b.Run("Values/"+benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			benchmark.setup(ht)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if values := ht.GetSlice("slice"); len(values) != 2 {
					b.Fatalf("GetSlice() length = %d, want 2", len(values))
				}
			}
		})
	}
}

func BenchmarkPackedTwoSliceStateTransitions(b *testing.B) {
	setup := func(b *testing.B) (*SliceStorage, int32) {
		b.Helper()
		storage := CreateSliceStorage()
		index := storage.addPacked(packedSliceValue{
			values: [2]interface{}{"first", "last"},
			length: 2,
			nonNil: true,
		})
		b.Cleanup(func() {
			storage.Del(index)
		})
		return storage, index
	}

	b.Run("PopPush", func(b *testing.B) {
		storage, index := setup(b)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if value, ok := storage.pop(index, false); !ok || value != "last" {
				b.Fatalf("pop() = %#v/%v, want last/true", value, ok)
			}
			nextIndex, err := storage.pushOneChecked(index, "last")
			if err != nil || nextIndex != index {
				b.Fatalf("pushOneChecked() = %d/%v, want %d/nil", nextIndex, err, index)
			}
		}
	})

	b.Run("ShiftPush", func(b *testing.B) {
		storage, index := setup(b)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if value, ok := storage.shift(index); !ok || value != "first" {
				b.Fatalf("shift() = %#v/%v, want first/true", value, ok)
			}
			nextIndex, err := storage.pushOneChecked(index, "first")
			if err != nil || nextIndex != index {
				b.Fatalf("pushOneChecked() = %d/%v, want %d/nil", nextIndex, err, index)
			}
			storage.twoValues[0].values[0], storage.twoValues[0].values[1] =
				storage.twoValues[0].values[1], storage.twoValues[0].values[0]
		}
	})
}

type benchmarkPackedSliceLengthMarker struct {
	length uint8
}

type benchmarkMarkerPackedTwoSlice struct {
	values [2]interface{}
}

var (
	benchmarkPackedSliceEmptyValue = &benchmarkPackedSliceLengthMarker{}
	benchmarkPackedSliceOneValue   = &benchmarkPackedSliceLengthMarker{length: 1}
)

func benchmarkMarkerPackedTwoSliceLength(data *benchmarkMarkerPackedTwoSlice) uint8 {
	if marker, ok := data.values[1].(*benchmarkPackedSliceLengthMarker); ok {
		return marker.length
	}
	return 2
}

func BenchmarkPackedTwoSliceLengthEncoding(b *testing.B) {
	b.Run("PopPush/InlineLength", func(b *testing.B) {
		data := packedTwoSlice{values: [2]interface{}{"first", "last"}, length: 2}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			last := data.length - 1
			value := data.values[last]
			data.values[last] = nil
			data.length--
			if value != "last" {
				b.Fatalf("popped value = %#v, want last", value)
			}
			data.values[data.length] = value
			data.length++
		}
	})

	b.Run("PopPush/TypedMarker", func(b *testing.B) {
		data := benchmarkMarkerPackedTwoSlice{values: [2]interface{}{"first", "last"}}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			length := benchmarkMarkerPackedTwoSliceLength(&data)
			last := length - 1
			value := data.values[last]
			data.values[last] = nil
			if length == 2 {
				data.values[1] = benchmarkPackedSliceOneValue
			} else {
				data.values[1] = benchmarkPackedSliceEmptyValue
			}
			if value != "last" {
				b.Fatalf("popped value = %#v, want last", value)
			}
			length = benchmarkMarkerPackedTwoSliceLength(&data)
			data.values[length] = value
		}
	})

	b.Run("ShiftPush/InlineLength", func(b *testing.B) {
		data := packedTwoSlice{values: [2]interface{}{"first", "last"}, length: 2}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			value := data.values[0]
			data.values[0] = data.values[1]
			data.values[1] = nil
			data.length--
			if value != "first" {
				b.Fatalf("shifted value = %#v, want first", value)
			}
			data.values[data.length] = value
			data.length++
			data.values[0], data.values[1] = data.values[1], data.values[0]
		}
	})

	b.Run("ShiftPush/TypedMarker", func(b *testing.B) {
		data := benchmarkMarkerPackedTwoSlice{values: [2]interface{}{"first", "last"}}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			length := benchmarkMarkerPackedTwoSliceLength(&data)
			value := data.values[0]
			data.values[0] = data.values[1]
			data.values[1] = benchmarkPackedSliceOneValue
			if value != "first" || length != 2 {
				b.Fatalf("shifted value/length = %#v/%d, want first/2", value, length)
			}
			length = benchmarkMarkerPackedTwoSliceLength(&data)
			data.values[length] = value
			data.values[0], data.values[1] = data.values[1], data.values[0]
		}
	})
}

func BenchmarkPackedSliceCommandGet(b *testing.B) {
	for _, benchmark := range []struct {
		name   string
		values Slice
	}{
		{name: "Nil", values: nil},
		{name: "Empty", values: Slice{}},
		{name: "OneString", values: Slice{"alpha"}},
		{name: "TwoStrings", values: Slice{"alpha", "beta"}},
		{name: "TwoNested", values: Slice{Map{"key": "value"}, Slice{1, "two"}}},
		{name: "Promoted3Strings", values: Slice{"alpha", "beta", "gamma"}},
		{name: "Promoted16Strings", values: benchmarkSliceStrings(16)},
		{name: "PromotedMixed", values: Slice{"<alpha>\n", nil, Map{"nested": Slice{true, int64(-7)}}, uint64(^uint64(0))}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			ht.UpsertSlice("slice", benchmark.values)
			request := CacheCommandRequest{Command: "GET", Key: "slice"}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				benchmarkCommandResponseSink = ht.ExecuteCommand(request)
			}
		})
	}
}

func benchmarkSliceStrings(count int) Slice {
	values := make(Slice, count)
	for index := range values {
		values[index] = fmt.Sprintf("value:%02d", index)
	}
	return values
}
