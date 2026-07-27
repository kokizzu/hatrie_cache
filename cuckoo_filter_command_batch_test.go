package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

func TestCuckooFilterBatchCommandExactMatchesGeneric(t *testing.T) {
	plainValues := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name   string
		setup  func(*testing.T, *HatTrie)
		values Slice
	}{
		{name: "FreshPlain64", values: plainValues},
		{
			name: "ExistingDuplicates",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertCuckooFilter("cuckoo:batch", 4096, 0.001); err != nil {
					t.Fatal(err)
				}
				if added, err := trie.AddCuckooFilterChecked("cuckoo:batch", "value-0"); err != nil || added != 1 {
					t.Fatalf("AddCuckooFilterChecked(existing) = %d/%v, want 1/nil", added, err)
				}
			},
			values: Slice{"value-0", "value-1", "value-1", "value-2"},
		},
		{
			name: "NearCapacity",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertCuckooFilter("cuckoo:batch", 16, 0.001); err != nil {
					t.Fatal(err)
				}
				for index := 0; index < 12; index++ {
					if _, err := trie.AddCuckooFilterChecked("cuckoo:batch", plainValues[index]); err != nil {
						t.Fatal(err)
					}
				}
			},
			values: plainValues[12:32],
		},
		{
			name: "ReplaceExpiringString",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("cuckoo:batch", "old")
				if !trie.Expire("cuckoo:batch", time.Hour) {
					t.Fatal("Expire(cuckoo:batch) = false, want true")
				}
			},
			values: plainValues,
		},
		{name: "EscapedAndStructured", values: Slice{"alpha", "<tag>", Map{"nested": "value"}}},
		{name: "InvalidLast", values: Slice{"alpha", func() {}}},
		{name: "Empty", values: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactResponse, exactSnapshot := runCuckooFilterBatchCommandFixture(t, "ADDCF", test.setup, test.values)
			genericResponse, genericSnapshot := runCuckooFilterBatchCommandFixture(t, " ADDCF", test.setup, test.values)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactSnapshot, genericSnapshot) {
				t.Fatalf("exact snapshot = %#v, generic = %#v", exactSnapshot, genericSnapshot)
			}
		})
	}
}

func runCuckooFilterBatchCommandFixture(t *testing.T, command string, setup func(*testing.T, *HatTrie), values Slice) (CacheCommandResponse, cuckooFilterSnapshot) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: command, Key: "cuckoo:batch", Values: values})

	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("cuckoo:batch")
	if raw == nil {
		return response, cuckooFilterSnapshot{}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsCuckooFilter() {
		t.Fatalf("cuckoo:batch type = %d, want Cuckoo filter", value.Type())
	}
	return response, trie.cuckooFilters.array[value.Index].Snapshot()
}

func BenchmarkCuckooFilterBatchCommandPath(b *testing.B) {
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
			{name: "Exact", value: "ADDCF"},
			{name: "Generic", value: " ADDCF"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				request := CacheCommandRequest{Command: command.value, Key: "cuckoo:batch", Values: workload.values}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := trie.UpsertCuckooFilter("cuckoo:batch", 4096, 0.001); err != nil {
						b.Fatal(err)
					}
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkCuckooFilterBatchCommandAlternating(b *testing.B) {
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
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:smallBatchSize]},
		{name: "Plain64", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "AllStructured", values: allStructured},
	} {
		b.Run(workload.name, func(b *testing.B) {
			exactTrie := CreateHatTrie()
			defer exactTrie.Destroy()
			genericTrie := CreateHatTrie()
			defer genericTrie.Destroy()
			exactRequest := CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:batch", Values: workload.values}
			genericRequest := CacheCommandRequest{Command: " ADDCF", Key: "cuckoo:batch", Values: workload.values}
			run := func(trie *HatTrie, request CacheCommandRequest) time.Duration {
				started := time.Now()
				for operation := 0; operation < blockOperations; operation++ {
					if err := trie.UpsertCuckooFilter("cuckoo:batch", 4096, 0.001); err != nil {
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
