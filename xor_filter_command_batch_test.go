package hatriecache

import (
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestXorFilterBatchCommandExactMatchesGeneric(t *testing.T) {
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
				if err := trie.UpsertXorFilter("xor:batch", 64); err != nil {
					t.Fatal(err)
				}
				if added, err := trie.AddXorFilterChecked("xor:batch", "value-0"); err != nil || added != 1 {
					t.Fatalf("AddXorFilterChecked(existing) = %d/%v, want 1/nil", added, err)
				}
			},
			values: Slice{"value-0", "value-1", "value-1", "value-2"},
		},
		{
			name: "ReplaceExpiringString",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("xor:batch", "old")
				if !trie.Expire("xor:batch", time.Hour) {
					t.Fatal("Expire(xor:batch) = false, want true")
				}
			},
			values: plainValues,
		},
		{
			name: "BuiltFilterRejectsBatch",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertXorFilter("xor:batch", 64); err != nil {
					t.Fatal(err)
				}
				if added, err := trie.AddXorFilterChecked("xor:batch", "existing"); err != nil || added != 1 {
					t.Fatalf("AddXorFilterChecked(existing) = %d/%v, want 1/nil", added, err)
				}
				if _, ok, err := trie.BuildXorFilter("xor:batch"); err != nil || !ok {
					t.Fatalf("BuildXorFilter() = ok:%t/%v, want true/nil", ok, err)
				}
			},
			values: plainValues,
		},
		{name: "EscapedAndStructured", values: Slice{"alpha", "<tag>", Map{"nested": "value"}}},
		{name: "InvalidLast", values: Slice{"alpha", func() {}}},
		{name: "Empty", values: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactResponse, exactSnapshot := runXorFilterBatchCommandFixture(t, "ADDXF", test.setup, test.values)
			genericResponse, genericSnapshot := runXorFilterBatchCommandFixture(t, " ADDXF", test.setup, test.values)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactSnapshot, genericSnapshot) {
				t.Fatalf("exact snapshot = %#v, generic = %#v", exactSnapshot, genericSnapshot)
			}
		})
	}
}

func TestXorFilterBatchCommandRetainsStringsAfterRequestRelease(t *testing.T) {
	trie := newTestTrie(t)
	values := benchmarkXorCommandValueSlice()
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "xor:batch", Values: values})
	if !response.OK || response.Value != "64" {
		t.Fatalf("ExecuteCommand(ADDXF) = %#v, want ok/64", response)
	}
	for index := range values {
		values[index] = nil
	}
	values = nil
	runtime.GC()

	trie.mu.RLock()
	raw := trie.tryLocation("xor:batch")
	if raw == nil {
		trie.mu.RUnlock()
		t.Fatal("xor:batch missing after request release")
	}
	var value HatValue
	value.fromValue(*raw)
	snapshot := trie.xorFilters.array[value.Index].Snapshot()
	trie.mu.RUnlock()
	if len(snapshot.Staged) != 64 {
		t.Fatalf("staged items = %d, want 64", len(snapshot.Staged))
	}
	for _, item := range snapshot.Staged {
		stringValue, ok := item.Value.(string)
		if !ok || xorFilterJSONStringKey(stringValue) != item.Key {
			t.Fatalf("staged item = %#v, want retained matching string", item)
		}
	}
}

func BenchmarkXorFilterBatchCommand64Path(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	plain8 := plain[:xorFilterLinearBatchDedup]
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Plain8Control", values: plain8},
		{name: "Plain", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
	} {
		for _, command := range []struct {
			name  string
			value string
		}{
			{name: "Exact", value: "ADDXF"},
			{name: "Generic", value: " ADDXF"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				request := CacheCommandRequest{Command: command.value, Key: "xor:batch", Values: workload.values}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := trie.UpsertXorFilter("xor:batch", 64); err != nil {
						b.Fatal(err)
					}
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkXorFilterBatchCommand64Alternating(b *testing.B) {
	const blockOperations = 8
	plain := benchmarkXorCommandValueSlice()
	plain8 := plain[:xorFilterLinearBatchDedup]
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Plain8Control", values: plain8},
		{name: "Plain", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
	} {
		b.Run(workload.name, func(b *testing.B) {
			exactTrie := CreateHatTrie()
			defer exactTrie.Destroy()
			genericTrie := CreateHatTrie()
			defer genericTrie.Destroy()
			exactRequest := CacheCommandRequest{Command: "ADDXF", Key: "xor:batch", Values: workload.values}
			genericRequest := CacheCommandRequest{Command: " ADDXF", Key: "xor:batch", Values: workload.values}
			run := func(trie *HatTrie, request CacheCommandRequest) time.Duration {
				started := time.Now()
				for operation := 0; operation < blockOperations; operation++ {
					if err := trie.UpsertXorFilter("xor:batch", 64); err != nil {
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

func benchmarkXorCommandValueSlice() Slice {
	strings := benchmarkXorCommandValues()
	values := make(Slice, len(strings))
	for index := range strings {
		values[index] = strings[index]
	}
	return values
}

func runXorFilterBatchCommandFixture(t *testing.T, command string, setup func(*testing.T, *HatTrie), values Slice) (CacheCommandResponse, xorFilterSnapshot) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: command, Key: "xor:batch", Values: values})

	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("xor:batch")
	if raw == nil {
		return response, xorFilterSnapshot{}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsXorFilter() {
		t.Fatalf("xor:batch type = %d, want XOR filter", value.Type())
	}
	return response, trie.xorFilters.array[value.Index].Snapshot()
}
