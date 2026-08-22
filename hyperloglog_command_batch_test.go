package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

type hyperLogLogCommandBatchState struct {
	present  bool
	hasTTL   bool
	count    uint64
	snapshot hyperLogLogSnapshot
}

func TestHyperLogLogBatchCommandExactMatchesGeneric(t *testing.T) {
	plainValues := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		values  Slice
	}{
		{name: "FreshPlain64", command: "ADDHLL", values: plainValues},
		{
			name:    "ExistingDuplicates",
			command: "ADDHLL",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertHyperLogLog("hll:batch", 10); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.AddHyperLogLogChecked("hll:batch", "value-0"); err != nil {
					t.Fatal(err)
				}
			},
			values: Slice{"value-0", "value-1", "value-1", "value-2"},
		},
		{name: "LowPrecisionCollisions", command: "HLLADD", setup: setupCommandBatchHyperLogLogPrecision(4), values: plainValues},
		{
			name:    "SaturatedObservations",
			command: "ADDHLL",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertHyperLogLog("hll:batch", 10); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.AddHyperLogLogChecked("hll:batch", "saturated-initial"); err != nil {
					t.Fatal(err)
				}
				trie.mu.Lock()
				defer trie.mu.Unlock()
				raw := trie.tryLocation("hll:batch")
				if raw == nil {
					t.Fatal("hll:batch missing after reserve")
				}
				var value HatValue
				value.fromValue(*raw)
				snapshot := trie.hyperLogLogs.array[value.Index].Snapshot()
				snapshot.Observations = ^uint64(0) - 1
				restored, err := newHyperLogLogDataFromSnapshot(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				trie.hyperLogLogs.PutData(value.Index, restored)
			},
			values: Slice{"saturated-a", "saturated-b", "saturated-c"},
		},
		{
			name:    "ReplaceExpiringString",
			command: "ADDHLL",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("hll:batch", "old")
				if !trie.Expire("hll:batch", time.Hour) {
					t.Fatal("Expire(hll:batch) = false, want true")
				}
			},
			values: plainValues,
		},
		{name: "EscapedAndStructured", command: "ADDHLL", values: Slice{"alpha", "<tag>", Map{"nested": "value"}}},
		{name: "EmptyStringValue", command: "ADDHLL", values: Slice{""}},
		{
			name:    "InvalidLastLeavesExistingSketchUnchanged",
			command: "ADDHLL",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				if err := trie.UpsertHyperLogLog("hll:batch", 10); err != nil {
					t.Fatal(err)
				}
				if _, err := trie.AddHyperLogLogChecked("hll:batch", "existing"); err != nil {
					t.Fatal(err)
				}
			},
			values: Slice{"alpha", func() {}},
		},
		{name: "Empty", command: "ADDHLL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactResponse, exactState := runHyperLogLogBatchCommandFixture(t, test.command, test.setup, test.values)
			genericResponse, genericState := runHyperLogLogBatchCommandFixture(t, " "+test.command, test.setup, test.values)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func setupCommandBatchHyperLogLogPrecision(precision uint8) func(*testing.T, *HatTrie) {
	return func(t *testing.T, trie *HatTrie) {
		t.Helper()
		if err := trie.UpsertHyperLogLog("hll:batch", precision); err != nil {
			t.Fatal(err)
		}
	}
}

func runHyperLogLogBatchCommandFixture(t *testing.T, command string, setup func(*testing.T, *HatTrie), values Slice) (CacheCommandResponse, hyperLogLogCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: command, Key: "hll:batch", Values: values})

	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("hll:batch")
	if raw == nil {
		return response, hyperLogLogCommandBatchState{}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsHyperLogLog() {
		t.Fatalf("hll:batch type = %d, want HyperLogLog", value.Type())
	}
	data := trie.hyperLogLogs.array[value.Index]
	return response, hyperLogLogCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		count:    data.Count(),
		snapshot: data.Snapshot(),
	}
}

func BenchmarkHyperLogLogExistingBatchCommandPath(b *testing.B) {
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
			{name: "Exact", value: "ADDHLL"},
			{name: "Generic", value: " ADDHLL"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				if err := trie.UpsertHyperLogLog("hll:batch", 14); err != nil {
					b.Fatal(err)
				}
				request := CacheCommandRequest{Command: command.value, Key: "hll:batch", Values: workload.values}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkHyperLogLogBatchCommandPath(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	for _, command := range []struct {
		name  string
		value string
	}{
		{name: "Exact", value: "ADDHLL"},
		{name: "Generic", value: " ADDHLL"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			request := CacheCommandRequest{Command: command.value, Key: "hll:batch", Values: plain}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.UpsertHyperLogLog("hll:batch", 14); err != nil {
					b.Fatal(err)
				}
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}
