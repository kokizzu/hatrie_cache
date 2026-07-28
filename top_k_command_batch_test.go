package hatriecache

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

type topKCommandBatchState struct {
	present  bool
	hasTTL   bool
	snapshot topKSnapshot
	info     TopKInfo
}

func TestTopKBatchCommandExactMatchesGeneric(t *testing.T) {
	plainValues := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		values  Slice
		subkey  string
		pairs   Map
	}{
		{name: "FreshPlain64", command: "ADDTOPK", values: plainValues},
		{name: "SingleValueResponse", command: "ADDTOPK", values: plainValues[:1]},
		{
			name:    "ExistingDuplicates",
			command: "ADDTOPK",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupTopKCommandBatch(t, trie, 16, Slice{"value-0", "value-1", "value-2"})
			},
			values: Slice{"value-0", "value-1", "value-1", "value-3"},
		},
		{
			name:    "CapacityTwoEviction",
			command: "TOPKADD",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupTopKCommandBatch(t, trie, 2, Slice{"seed-a", "seed-b"})
			},
			values: plainValues[:8],
			subkey: "3",
		},
		{name: "SubkeyCount", command: "ADDTOPK", values: plainValues[:8], subkey: "7"},
		{name: "PairsCountPrecedence", command: "ADDTOPK", values: plainValues[:8], subkey: "7", pairs: Map{"count": "5"}},
		{
			name:    "SaturatedCounts",
			command: "ADDTOPK",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertTopK("topk:batch", 4); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.AddTopKChecked("topk:batch", "existing", ^uint64(0)-1); err != nil {
					t.Fatal(err)
				}
			},
			values: Slice{"existing", "new"},
			subkey: "9",
		},
		{
			name:    "ReplaceExpiringString",
			command: "ADDTOPK",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("topk:batch", "old")
				if !trie.Expire("topk:batch", time.Hour) {
					t.Fatal("Expire(topk:batch) = false, want true")
				}
			},
			values: plainValues,
		},
		{name: "EscapedAndStructured", command: "ADDTOPK", values: Slice{"alpha", "<tag>", Map{"nested": "value"}}},
		{name: "StructuredFirst", command: "ADDTOPK", values: Slice{Map{"nested": "value"}, "alpha", "beta"}},
		{name: "LongCanonicalStrings", command: "ADDTOPK", values: Slice{strings.Repeat("a", 4096), strings.Repeat("b", 4096)}},
		{name: "EmptyStringValue", command: "ADDTOPK", values: Slice{""}},
		{
			name:    "InvalidLastLeavesExistingSketchUnchanged",
			command: "ADDTOPK",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupTopKCommandBatch(t, trie, 16, Slice{"existing"})
			},
			values: Slice{"alpha", func() {}},
		},
		{name: "InvalidCount", command: "ADDTOPK", values: plainValues[:2], subkey: "invalid"},
		{name: "Empty", command: "ADDTOPK"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactResponse, exactState := runTopKBatchCommandFixture(t, test.command, test.setup, test.values, test.subkey, test.pairs)
			genericResponse, genericState := runTopKBatchCommandFixture(t, " "+test.command, test.setup, test.values, test.subkey, test.pairs)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func setupTopKCommandBatch(t *testing.T, trie *HatTrie, capacity uint64, values Slice) {
	t.Helper()
	if err := trie.UpsertTopK("topk:batch", capacity); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if _, err := trie.AddTopKChecked("topk:batch", value, 1); err != nil {
			t.Fatal(err)
		}
	}
}

func runTopKBatchCommandFixture(t *testing.T, command string, setup func(*testing.T, *HatTrie), values Slice, subkey string, pairs Map) (CacheCommandResponse, topKCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{
		Command: command,
		Key:     "topk:batch",
		Values:  values,
		Subkey:  subkey,
		Pairs:   pairs,
	})

	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("topk:batch")
	if raw == nil {
		return response, topKCommandBatchState{}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsTopK() {
		t.Fatalf("topk:batch type = %d, want TopK", value.Type())
	}
	data := trie.topKs.array[value.Index]
	return response, topKCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		snapshot: data.Snapshot(),
		info:     data.Info(),
	}
}

func BenchmarkTopKExistingBatchCommandPath(b *testing.B) {
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
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:smallBatchSize]},
		{name: "Plain64", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "AllStructured", values: allStructured},
	} {
		for _, command := range []struct {
			name  string
			value string
		}{
			{name: "Exact", value: "ADDTOPK"},
			{name: "Generic", value: " ADDTOPK"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				setupTopKCommandBatchBenchmark(b, trie, plain)
				request := CacheCommandRequest{Command: command.value, Key: "topk:batch", Values: workload.values}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkTopKBatchCommandPath(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	for _, command := range []struct {
		name  string
		value string
	}{
		{name: "Exact", value: "ADDTOPK"},
		{name: "Generic", value: " ADDTOPK"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			request := CacheCommandRequest{Command: command.value, Key: "topk:batch", Values: plain}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.UpsertTopK("topk:batch", 128); err != nil {
					b.Fatal(err)
				}
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}

func setupTopKCommandBatchBenchmark(b *testing.B, trie *HatTrie, values Slice) {
	b.Helper()
	if err := trie.UpsertTopK("topk:batch", 128); err != nil {
		b.Fatal(err)
	}
	if _, err := trie.AddTopKChecked("topk:batch", values[0], 1, values[1:]...); err != nil {
		b.Fatal(err)
	}
}
