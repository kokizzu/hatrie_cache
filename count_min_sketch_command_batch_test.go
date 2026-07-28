package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

type countMinSketchCommandBatchState struct {
	present  bool
	hasTTL   bool
	snapshot countMinSketchSnapshot
}

func TestCountMinSketchBatchCommandExactMatchesGeneric(t *testing.T) {
	plainValues := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name    string
		setup   func(*testing.T, *HatTrie)
		request CacheCommandRequest
	}{
		{
			name:    "FreshPlain64",
			request: CacheCommandRequest{Values: plainValues},
		},
		{
			name: "ExistingDuplicatesWithCount",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertCountMinSketch("count-min:batch", 64, 4); err != nil {
					t.Fatal(err)
				}
				if estimate, err := trie.IncrementCountMinSketchChecked("count-min:batch", "value-0", 3); err != nil || estimate != 3 {
					t.Fatalf("IncrementCountMinSketchChecked(existing) = %d/%v, want 3/nil", estimate, err)
				}
			},
			request: CacheCommandRequest{Values: Slice{"value-0", "value-1", "value-1", "value-2"}, Subkey: "7"},
		},
		{
			name: "PairCountOverridesSubkey",
			request: CacheCommandRequest{
				Values: Slice{"pair-a", "pair-b"},
				Subkey: "3",
				Pairs:  Map{"count": uint64(9)},
			},
		},
		{
			name: "SaturatedCounter",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertCountMinSketch("count-min:batch", 16, 4); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.IncrementCountMinSketchChecked("count-min:batch", "saturated", maxCountMinSketchCounter-2); err != nil {
					t.Fatal(err)
				}
			},
			request: CacheCommandRequest{Values: Slice{"saturated", "fresh"}, Subkey: "5"},
		},
		{
			name: "ReplaceExpiringString",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("count-min:batch", "old")
				if !trie.Expire("count-min:batch", time.Hour) {
					t.Fatal("Expire(count-min:batch) = false, want true")
				}
			},
			request: CacheCommandRequest{Values: plainValues},
		},
		{
			name:    "EscapedAndStructured",
			request: CacheCommandRequest{Values: Slice{"alpha", "<tag>", Map{"nested": "value"}}, Subkey: "2"},
		},
		{
			name:    "EmptyStringValue",
			request: CacheCommandRequest{Values: Slice{""}},
		},
		{
			name: "InvalidLastLeavesExistingSketchUnchanged",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertCountMinSketch("count-min:batch", 64, 4); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.IncrementCountMinSketchChecked("count-min:batch", "existing", 11); err != nil {
					t.Fatal(err)
				}
			},
			request: CacheCommandRequest{Values: Slice{"alpha", func() {}}},
		},
		{
			name:    "InvalidCount",
			request: CacheCommandRequest{Values: plainValues[:2], Subkey: "0"},
		},
		{
			name:    "Empty",
			request: CacheCommandRequest{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactRequest := test.request
			exactRequest.Command = "INCRCMS"
			genericRequest := test.request
			genericRequest.Command = " INCRCMS"
			exactResponse, exactState := runCountMinSketchBatchCommandFixture(t, exactRequest, test.setup)
			genericResponse, genericState := runCountMinSketchBatchCommandFixture(t, genericRequest, test.setup)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func runCountMinSketchBatchCommandFixture(t *testing.T, request CacheCommandRequest, setup func(*testing.T, *HatTrie)) (CacheCommandResponse, countMinSketchCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	request.Key = "count-min:batch"
	response := trie.ExecuteCommand(request)

	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("count-min:batch")
	if raw == nil {
		return response, countMinSketchCommandBatchState{}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsCountMinSketch() {
		t.Fatalf("count-min:batch type = %d, want Count-Min Sketch", value.Type())
	}
	return response, countMinSketchCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		snapshot: trie.countMinSketches.array[value.Index].Snapshot(),
	}
}

func BenchmarkCountMinSketchBatchCommandPath(b *testing.B) {
	const smallBatchSize = 8
	plain := benchmarkXorCommandValueSlice()
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}
	allStructured := make(Slice, len(plain))
	for index := range allStructured {
		allStructured[index] = Map{"index": index}
	}

	for _, workload := range []struct {
		name   string
		values Slice
		count  string
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:smallBatchSize]},
		{name: "Plain64", values: plain},
		{name: "Plain64Count7", values: plain, count: "7"},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "AllStructured", values: allStructured},
	} {
		for _, command := range []struct {
			name  string
			value string
		}{
			{name: "Exact", value: "INCRCMS"},
			{name: "Generic", value: " INCRCMS"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				request := CacheCommandRequest{Command: command.value, Key: "count-min:batch", Values: workload.values, Subkey: workload.count}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := trie.UpsertCountMinSketch("count-min:batch", 2048, 4); err != nil {
						b.Fatal(err)
					}
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkCountMinSketchExistingBatchCommandPath(b *testing.B) {
	const smallBatchSize = 8
	plain := benchmarkXorCommandValueSlice()
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}
	allStructured := make(Slice, len(plain))
	for index := range allStructured {
		allStructured[index] = Map{"index": index}
	}

	for _, workload := range []struct {
		name   string
		values Slice
		count  string
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:smallBatchSize]},
		{name: "Plain64", values: plain},
		{name: "Plain64Count7", values: plain, count: "7"},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "AllStructured", values: allStructured},
	} {
		for _, command := range []struct {
			name  string
			value string
		}{
			{name: "Exact", value: "INCRCMS"},
			{name: "Generic", value: " INCRCMS"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				if err := trie.UpsertCountMinSketch("count-min:batch", 2048, 4); err != nil {
					b.Fatal(err)
				}
				request := CacheCommandRequest{Command: command.value, Key: "count-min:batch", Values: workload.values, Subkey: workload.count}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkCountMinSketchBatchCommandAlternating(b *testing.B) {
	const blockOperations = 8
	const smallBatchSize = 8
	plain := benchmarkXorCommandValueSlice()
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}
	allStructured := make(Slice, len(plain))
	for index := range allStructured {
		allStructured[index] = Map{"index": index}
	}

	for _, workload := range []struct {
		name   string
		values Slice
		count  string
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:smallBatchSize]},
		{name: "Plain64", values: plain},
		{name: "Plain64Count7", values: plain, count: "7"},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "AllStructured", values: allStructured},
	} {
		b.Run(workload.name, func(b *testing.B) {
			exactTrie := CreateHatTrie()
			defer exactTrie.Destroy()
			genericTrie := CreateHatTrie()
			defer genericTrie.Destroy()
			exactRequest := CacheCommandRequest{Command: "INCRCMS", Key: "count-min:batch", Values: workload.values, Subkey: workload.count}
			genericRequest := CacheCommandRequest{Command: " INCRCMS", Key: "count-min:batch", Values: workload.values, Subkey: workload.count}
			run := func(trie *HatTrie, request CacheCommandRequest) time.Duration {
				started := time.Now()
				for operation := 0; operation < blockOperations; operation++ {
					if err := trie.UpsertCountMinSketch("count-min:batch", 2048, 4); err != nil {
						b.Fatal(err)
					}
					response := trie.ExecuteCommand(request)
					if !response.OK {
						b.Fatalf("ExecuteCommand(%q) = %#v, want ok", request.Command, response)
					}
				}
				return time.Since(started)
			}

			var exactDuration time.Duration
			var genericDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					exactDuration += run(exactTrie, exactRequest)
					genericDuration += run(genericTrie, genericRequest)
				} else {
					genericDuration += run(genericTrie, genericRequest)
					exactDuration += run(exactTrie, exactRequest)
				}
			}
			operations := float64(b.N * blockOperations)
			b.ReportMetric(float64(exactDuration.Nanoseconds())/operations, "exact-ns/op")
			b.ReportMetric(float64(genericDuration.Nanoseconds())/operations, "generic-ns/op")
		})
	}
}
