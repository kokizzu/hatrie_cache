package hatriecache

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

type reservoirSampleCommandBatchState struct {
	present  bool
	hasTTL   bool
	writes   uint64
	snapshot reservoirSampleSnapshot
	info     ReservoirSampleInfo
}

func TestReservoirSampleBatchCommandExactMatchesGeneric(t *testing.T) {
	plainValues := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		values  Slice
	}{
		{name: "FreshPlain64", command: "ADDRS", values: plainValues},
		{name: "SingleValueResponse", command: "ADDRS", values: plainValues[:1]},
		{
			name:    "ExistingCapacityEviction",
			command: "RSADD",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupReservoirSampleCommandBatch(t, trie, 2, Slice{"seed-a", "seed-b"})
			},
			values: plainValues[:8],
		},
		{
			name:    "ReplaceExpiringString",
			command: "ADDRS",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("reservoir:batch", "old")
				if !trie.Expire("reservoir:batch", time.Hour) {
					t.Fatal("Expire(reservoir:batch) = false, want true")
				}
			},
			values: plainValues,
		},
		{name: "EscapedAndStructured", command: "ADDRS", values: Slice{"alpha", "<tag>", Map{"nested": "value"}}},
		{name: "StructuredFirst", command: "ADDRS", values: Slice{Map{"nested": "value"}, "alpha", "beta"}},
		{name: "LongCanonicalStrings", command: "ADDRS", values: Slice{strings.Repeat("a", 4096), strings.Repeat("b", 4096)}},
		{name: "EmptyStringValue", command: "ADDRS", values: Slice{""}},
		{
			name:    "InvalidLastLeavesExistingSampleUnchanged",
			command: "ADDRS",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupReservoirSampleCommandBatch(t, trie, 16, Slice{"existing"})
			},
			values: Slice{"alpha", func() {}},
		},
		{
			name:    "SequenceOverflowLeavesExistingSampleUnchanged",
			command: "ADDRS",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupReservoirSampleCommandBatch(t, trie, 4, Slice{"existing"})
				trie.mu.Lock()
				hval := trie.peekCachedLocked("reservoir:batch")
				trie.reservoirSamples.array[hval.Index].seen = ^uint64(0) - 1
				trie.mu.Unlock()
			},
			values: Slice{"last", "overflow"},
		},
		{name: "InvalidFreshValue", command: "ADDRS", values: Slice{"alpha", func() {}}},
		{name: "Empty", command: "ADDRS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactResponse, exactState := runReservoirSampleBatchCommandFixture(t, test.command, test.setup, test.values)
			genericResponse, genericState := runReservoirSampleBatchCommandFixture(t, " "+test.command, test.setup, test.values)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func TestReservoirSampleBatchCommandRetainsCallerValues(t *testing.T) {
	trie := newTestTrie(t)
	nested := Map{"path": "/api/cache"}
	values := Slice{strings.Repeat("plain-", 128), nested}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "ADDRS", Key: "reservoir:batch", Values: values})
	if !response.OK {
		t.Fatalf("ADDRS response = %#v, want ok", response)
	}

	nested["path"] = "mutated"
	values[0] = nil
	values[1] = nil
	values = nil
	runtime.GC()

	items := trie.GetReservoirSample("reservoir:batch")
	if len(items) != 2 {
		t.Fatalf("GetReservoirSample() len = %d, want 2", len(items))
	}
	foundString := false
	foundMap := false
	for _, item := range items {
		switch value := item.Value.(type) {
		case string:
			foundString = value == strings.Repeat("plain-", 128)
		case Map:
			foundMap = value["path"] == "/api/cache"
		}
	}
	if !foundString || !foundMap {
		t.Fatalf("retained values = %#v, want independent string and map", items)
	}
}

func TestReservoirSampleCommandBatchDataMatchesReference(t *testing.T) {
	plain := benchmarkXorCommandValueSlice()
	for _, test := range []struct {
		name     string
		capacity uint64
		seen     uint64
		seed     Slice
		values   Slice
	}{
		{name: "Plain1", capacity: 128, seed: plain, values: plain[:1]},
		{name: "Plain64", capacity: 128, seed: plain, values: plain},
		{name: "Eviction", capacity: 2, seed: Slice{"seed-a", "seed-b"}, values: plain[:8]},
		{name: "EscapedLast", capacity: 128, values: Slice{"alpha", "beta", "<tag>"}},
		{name: "StructuredLast", capacity: 128, values: Slice{"alpha", "beta", Map{"nested": Slice{"value"}}}},
		{name: "StructuredFirst", capacity: 128, values: Slice{Map{"nested": "value"}, "alpha", "beta"}},
		{name: "InvalidLast", capacity: 128, seed: Slice{"existing"}, values: Slice{"alpha", func() {}}},
		{name: "SequenceOverflow", capacity: 4, seen: ^uint64(0) - 1, seed: Slice{"existing"}, values: Slice{"last", "overflow"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := newReservoirSampleData(test.capacity)
			if err != nil {
				t.Fatal(err)
			}
			reference, err := newReservoirSampleData(test.capacity)
			if err != nil {
				t.Fatal(err)
			}
			if len(test.seed) != 0 {
				if _, err := candidate.AddOneChecked(test.seed[0], test.seed[1:]...); err != nil {
					t.Fatal(err)
				}
				if _, err := reference.AddOneChecked(test.seed[0], test.seed[1:]...); err != nil {
					t.Fatal(err)
				}
			}
			if test.seen != 0 {
				candidate.seen = test.seen
				reference.seen = test.seen
			}

			got, gotErr := candidate.addCommandBatchChecked(test.values[0], test.values[1:])
			want, wantErr := reference.AddOneChecked(test.values[0], test.values[1:]...)
			if got != want || errorString(gotErr) != errorString(wantErr) {
				t.Fatalf("candidate = %#v/%v, reference = %#v/%v", got, gotErr, want, wantErr)
			}
			if gotSnapshot, wantSnapshot := candidate.Snapshot(), reference.Snapshot(); !reflect.DeepEqual(gotSnapshot, wantSnapshot) {
				t.Fatalf("candidate snapshot = %#v, reference = %#v", gotSnapshot, wantSnapshot)
			}
		})
	}
}

func TestReservoirSampleBatchCommandSupportsLocalPartitions(t *testing.T) {
	values := Slice{"alpha", "beta", "gamma", "<tag>", Map{"nested": "value"}}
	single := newTestTrie(t)
	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}

	want := single.ExecuteCommand(CacheCommandRequest{Command: "ADDRS", Key: "reservoir:partitioned", Values: values})
	got := partitioned.ExecuteCommand(CacheCommandRequest{Command: "ADDRS", Key: "reservoir:partitioned", Values: values})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned response = %#v, single = %#v", got, want)
	}
	gotItems, gotOK, gotErr := partitioned.GetReservoirSampleChecked("reservoir:partitioned")
	wantItems, wantOK, wantErr := single.GetReservoirSampleChecked("reservoir:partitioned")
	if !reflect.DeepEqual(gotItems, wantItems) || gotOK != wantOK || errorString(gotErr) != errorString(wantErr) {
		t.Fatalf("partitioned items = %#v/%v/%v, single = %#v/%v/%v", gotItems, gotOK, gotErr, wantItems, wantOK, wantErr)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func setupReservoirSampleCommandBatch(t *testing.T, trie *HatTrie, capacity uint64, values Slice) {
	t.Helper()
	if err := trie.UpsertReservoirSample("reservoir:batch", capacity); err != nil {
		t.Fatal(err)
	}
	if len(values) == 0 {
		return
	}
	if _, err := trie.AddReservoirSampleChecked("reservoir:batch", values[0], values[1:]...); err != nil {
		t.Fatal(err)
	}
}

func runReservoirSampleBatchCommandFixture(t *testing.T, command string, setup func(*testing.T, *HatTrie), values Slice) (CacheCommandResponse, reservoirSampleCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: command, Key: "reservoir:batch", Values: values})

	stats := trie.Stats()
	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("reservoir:batch")
	if raw == nil {
		return response, reservoirSampleCommandBatchState{writes: stats.Writes}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsReservoirSample() {
		t.Fatalf("reservoir:batch type = %d, want reservoir sample", value.Type())
	}
	data := trie.reservoirSamples.array[value.Index]
	return response, reservoirSampleCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		writes:   stats.Writes,
		snapshot: data.Snapshot(),
		info:     data.Info(),
	}
}

func BenchmarkReservoirSampleExistingBatchCommandPath(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}
	allStructured := make(Slice, len(plain))
	for index := range allStructured {
		allStructured[index] = Map{"index": index}
	}
	manyStructuredTail := append(Slice(nil), allStructured...)
	manyStructuredTail[0] = plain[0]

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:8]},
		{name: "Plain64", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "ManyStructuredTail", values: manyStructuredTail},
		{name: "AllStructured", values: allStructured},
	} {
		for _, command := range []struct {
			name  string
			value string
		}{
			{name: "Exact", value: "ADDRS"},
			{name: "Generic", value: " ADDRS"},
		} {
			b.Run(workload.name+"/"+command.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				setupReservoirSampleCommandBatchBenchmark(b, trie, plain)
				request := CacheCommandRequest{Command: command.value, Key: "reservoir:batch", Values: workload.values}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}

func BenchmarkReservoirSampleBatchCommandPath(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	for _, command := range []struct {
		name  string
		value string
	}{
		{name: "Exact", value: "ADDRS"},
		{name: "Generic", value: " ADDRS"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			request := CacheCommandRequest{Command: command.value, Key: "reservoir:batch", Values: plain}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.UpsertReservoirSample("reservoir:batch", 128); err != nil {
					b.Fatal(err)
				}
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}

var benchmarkReservoirSampleCommandBatchUpdateSink ReservoirSampleUpdate

func BenchmarkReservoirSampleCommandBatchAlternating(b *testing.B) {
	plain := benchmarkXorCommandValueSlice()
	escapedLast := append(Slice(nil), plain...)
	escapedLast[len(escapedLast)-1] = "<tag>"
	structuredLast := append(Slice(nil), plain...)
	structuredLast[len(structuredLast)-1] = Map{"nested": "value"}
	allStructured := make(Slice, len(plain))
	for index := range allStructured {
		allStructured[index] = Map{"index": index}
	}
	manyStructuredTail := append(Slice(nil), allStructured...)
	manyStructuredTail[0] = plain[0]

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Plain1", values: plain[:1]},
		{name: "Plain2", values: plain[:2]},
		{name: "Plain8", values: plain[:8]},
		{name: "Plain64", values: plain},
		{name: "EscapedLast", values: escapedLast},
		{name: "StructuredLast", values: structuredLast},
		{name: "ManyStructuredTail", values: manyStructuredTail},
		{name: "AllStructured", values: allStructured},
	} {
		b.Run(workload.name, func(b *testing.B) {
			candidate := newDefaultReservoirSampleData()
			reference := newDefaultReservoirSampleData()
			if _, err := candidate.AddOneChecked(plain[0], plain[1:]...); err != nil {
				b.Fatal(err)
			}
			if _, err := reference.AddOneChecked(plain[0], plain[1:]...); err != nil {
				b.Fatal(err)
			}

			const operationsPerBlock = 64
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkReservoirSampleCommandCandidateBlock(b, &candidate, workload.values)
					referenceDuration += benchmarkReservoirSampleCommandReferenceBlock(b, &reference, workload.values)
				} else {
					referenceDuration += benchmarkReservoirSampleCommandReferenceBlock(b, &reference, workload.values)
					candidateDuration += benchmarkReservoirSampleCommandCandidateBlock(b, &candidate, workload.values)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func benchmarkReservoirSampleCommandCandidateBlock(b *testing.B, sample *reservoirSampleData, values Slice) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < 64; operation++ {
		update, err := sample.addCommandBatchChecked(values[0], values[1:])
		if err != nil {
			b.Fatal(err)
		}
		benchmarkReservoirSampleCommandBatchUpdateSink = update
	}
	return time.Since(start)
}

func benchmarkReservoirSampleCommandReferenceBlock(b *testing.B, sample *reservoirSampleData, values Slice) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < 64; operation++ {
		update, err := sample.AddOneChecked(values[0], values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkReservoirSampleCommandBatchUpdateSink = update
	}
	return time.Since(start)
}

func setupReservoirSampleCommandBatchBenchmark(b *testing.B, trie *HatTrie, values Slice) {
	b.Helper()
	if err := trie.UpsertReservoirSample("reservoir:batch", 128); err != nil {
		b.Fatal(err)
	}
	if _, err := trie.AddReservoirSampleChecked("reservoir:batch", values[0], values[1:]...); err != nil {
		b.Fatal(err)
	}
}
