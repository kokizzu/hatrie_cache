package hatriecache

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type roaringBitmapCommandBatchState struct {
	present  bool
	hasTTL   bool
	writes   uint64
	snapshot roaringBitmapSnapshot
	info     RoaringBitmapInfo
}

func TestRoaringBitmapAddBatchCommandExactMatchesGeneric(t *testing.T) {
	plain := benchmarkRoaringBitmapCommandValues(128)
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		request CacheCommandRequest
	}{
		{name: "FreshUint32", command: "ADDRB", request: CacheCommandRequest{Values: plain[:64]}},
		{name: "Alias", command: "RBADD", request: CacheCommandRequest{Values: plain[:8]}},
		{name: "Existing", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: plain[:64]}},
		{name: "LargeFallback", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: plain}},
		{
			name:    "ReplaceExpiringString",
			command: "ADDRB",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("roaring:batch", "old")
				if !trie.Expire("roaring:batch", time.Hour) {
					t.Fatal("Expire(roaring:batch) = false, want true")
				}
			},
			request: CacheCommandRequest{Values: plain[:64]},
		},
		{
			name:    "MixedNumericRepresentations",
			command: "ADDRB",
			request: CacheCommandRequest{Values: Slice{
				uint32(1), uint64(2), uint(3), int(4), int32(5), int64(6),
				float64(7), json.Number("8"), " 9 ",
			}},
		},
		{name: "ScalarValue", command: "ADDRB", request: CacheCommandRequest{Value: " 42 "}},
		{name: "InvalidTail", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: Slice{uint32(100), "-1"}}},
		{name: "FractionalTail", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: Slice{uint32(100), 1.5}}},
		{name: "InfiniteTail", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: Slice{uint32(100), math.Inf(1)}}},
		{name: "OverflowTail", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: Slice{uint32(100), uint64(math.MaxUint32) + 1}}},
		{name: "UnsupportedTail", command: "ADDRB", setup: setupExistingRoaringBitmapCommandBatch, request: CacheCommandRequest{Values: Slice{uint32(100), Map{"value": 2}}}},
		{name: "InvalidFresh", command: "ADDRB", request: CacheCommandRequest{Values: Slice{uint32(100), func() {}}}},
		{name: "Empty", command: "ADDRB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactRequest := test.request
			exactRequest.Command = test.command
			exactResponse, exactState := runRoaringBitmapAddBatchCommandFixture(t, test.setup, exactRequest)

			genericRequest := test.request
			genericRequest.Command = " " + test.command
			genericResponse, genericState := runRoaringBitmapAddBatchCommandFixture(t, test.setup, genericRequest)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func TestRoaringBitmapAddBatchCommandMatchesPublicAPI(t *testing.T) {
	values := []uint32{0, 1, 65535, 65536, 65543, math.MaxUint32}
	requestValues := make(Slice, len(values))
	for index, value := range values {
		requestValues[index] = value
	}

	commandTrie := newTestTrie(t)
	publicTrie := newTestTrie(t)
	setupRoaringBitmapCommandBatch(t, commandTrie)
	setupRoaringBitmapCommandBatch(t, publicTrie)

	response := commandTrie.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "roaring:batch", Values: requestValues})
	if !response.OK {
		t.Fatalf("ADDRB response = %#v, want ok", response)
	}
	wantAdded, err := publicTrie.AddRoaringBitmapChecked("roaring:batch", values[0], values[1:]...)
	if err != nil {
		t.Fatal(err)
	}
	wantResponse := CacheCommandResponse{OK: true, Message: "added roaring bitmap values", Value: strconv.Itoa(wantAdded)}
	if !reflect.DeepEqual(response, wantResponse) {
		t.Fatalf("command response = %#v, public response = %#v", response, wantResponse)
	}
	if got, want := roaringBitmapCommandState(t, commandTrie), roaringBitmapCommandState(t, publicTrie); !reflect.DeepEqual(got, want) {
		t.Fatalf("command state = %#v, public state = %#v", got, want)
	}
}

func TestRoaringBitmapAddBatchCommandSupportsLocalPartitions(t *testing.T) {
	values := benchmarkRoaringBitmapCommandValues(64)
	single := newTestTrie(t)
	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}

	want := single.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "roaring:partitioned", Values: values})
	got := partitioned.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "roaring:partitioned", Values: values})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned response = %#v, single = %#v", got, want)
	}
	gotValues, gotOK, gotErr := partitioned.GetRoaringBitmapChecked("roaring:partitioned")
	wantValues, wantOK, wantErr := single.GetRoaringBitmapChecked("roaring:partitioned")
	if !reflect.DeepEqual(gotValues, wantValues) || gotOK != wantOK || errorString(gotErr) != errorString(wantErr) {
		t.Fatalf("GetRoaringBitmapChecked partitioned = %#v/%v/%v, single = %#v/%v/%v", gotValues, gotOK, gotErr, wantValues, wantOK, wantErr)
	}
}

func setupExistingRoaringBitmapCommandBatch(t *testing.T, trie *HatTrie) {
	t.Helper()
	setupRoaringBitmapCommandBatch(t, trie)
}

func setupRoaringBitmapCommandBatch(t testing.TB, trie *HatTrie) {
	t.Helper()
	if err := trie.UpsertRoaringBitmapChecked("roaring:batch"); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.AddRoaringBitmapChecked("roaring:batch", 7, 65543, math.MaxUint32); err != nil {
		t.Fatal(err)
	}
}

func runRoaringBitmapAddBatchCommandFixture(t *testing.T, setup func(*testing.T, *HatTrie), request CacheCommandRequest) (CacheCommandResponse, roaringBitmapCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	request.Key = "roaring:batch"
	response := trie.ExecuteCommand(request)
	return response, roaringBitmapCommandState(t, trie)
}

func roaringBitmapCommandState(t testing.TB, trie *HatTrie) roaringBitmapCommandBatchState {
	t.Helper()
	stats := trie.Stats()
	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("roaring:batch")
	if raw == nil {
		return roaringBitmapCommandBatchState{writes: stats.Writes}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsRoaringBitmap() {
		t.Fatalf("roaring:batch type = %d, want roaring bitmap", value.Type())
	}
	data := trie.roaringBitmaps.array[value.Index]
	return roaringBitmapCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		writes:   stats.Writes,
		snapshot: data.Snapshot(),
		info:     data.Info(),
	}
}

func benchmarkRoaringBitmapCommandValues(count int) Slice {
	values := make(Slice, count)
	for index := range values {
		values[index] = uint32(index*65537 + 17)
	}
	return values
}

func BenchmarkRoaringBitmapExistingAddBatchCommandPath(b *testing.B) {
	plain := benchmarkRoaringBitmapCommandValues(128)
	mixed := make(Slice, 64)
	for index := range mixed {
		value := plain[index].(uint32)
		switch index % 4 {
		case 0:
			mixed[index] = value
		case 1:
			mixed[index] = uint64(value)
		case 2:
			mixed[index] = json.Number(strconv.FormatUint(uint64(value), 10))
		default:
			mixed[index] = strconv.FormatUint(uint64(value), 10)
		}
	}

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Uint32_1", values: plain[:1]},
		{name: "Uint32_2", values: plain[:2]},
		{name: "Uint32_8", values: plain[:8]},
		{name: "Uint32_64", values: plain[:64]},
		{name: "Uint32_128", values: plain},
		{name: "Mixed64", values: mixed},
	} {
		b.Run(workload.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupRoaringBitmapCommandBatch(b, trie)
			request := CacheCommandRequest{Command: "ADDRB", Key: "roaring:batch", Values: workload.values}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}

	for _, command := range []struct {
		name    string
		command string
	}{
		{name: "ExactScalarValue", command: "ADDRB"},
		{name: "GenericScalarValue", command: " ADDRB"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupRoaringBitmapCommandBatch(b, trie)
			request := CacheCommandRequest{Command: command.command, Key: "roaring:batch", Value: "17"}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}

func BenchmarkRoaringBitmapFreshAddBatchCommandPath(b *testing.B) {
	values := benchmarkRoaringBitmapCommandValues(64)
	trie := CreateHatTrie()
	defer trie.Destroy()
	request := CacheCommandRequest{Command: "ADDRB", Key: "roaring:batch", Values: values}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := trie.UpsertRoaringBitmapChecked("roaring:batch"); err != nil {
			b.Fatal(err)
		}
		benchmarkExecuteCommand(b, trie, request)
	}
}

var benchmarkRoaringBitmapCommandResponseSink CacheCommandResponse

func BenchmarkRoaringBitmapAddCommandBatchAlternating(b *testing.B) {
	plain := benchmarkRoaringBitmapCommandValues(128)
	mixed := make(Slice, 64)
	for index := range mixed {
		value := plain[index].(uint32)
		switch index % 4 {
		case 0:
			mixed[index] = value
		case 1:
			mixed[index] = uint64(value)
		case 2:
			mixed[index] = json.Number(strconv.FormatUint(uint64(value), 10))
		default:
			mixed[index] = strconv.FormatUint(uint64(value), 10)
		}
	}

	for _, workload := range []struct {
		name   string
		values Slice
		fresh  bool
	}{
		{name: "Uint32_1", values: plain[:1]},
		{name: "Uint32_2", values: plain[:2]},
		{name: "Uint32_8", values: plain[:8]},
		{name: "Uint32_64", values: plain[:64]},
		{name: "Uint32_128", values: plain},
		{name: "Mixed64", values: mixed},
		{name: "FreshUint32_64", values: plain[:64], fresh: true},
	} {
		b.Run(workload.name, func(b *testing.B) {
			candidate := CreateHatTrie()
			defer candidate.Destroy()
			reference := CreateHatTrie()
			defer reference.Destroy()
			setupRoaringBitmapCommandBatch(b, candidate)
			setupRoaringBitmapCommandBatch(b, reference)
			request := CacheCommandRequest{Command: "ADDRB", Key: "roaring:batch", Values: workload.values}

			const operationsPerBlock = 64
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkRoaringBitmapCommandCandidateBlock(b, candidate, request, operationsPerBlock, workload.fresh)
					referenceDuration += benchmarkRoaringBitmapCommandReferenceBlock(b, reference, request, operationsPerBlock, workload.fresh)
				} else {
					referenceDuration += benchmarkRoaringBitmapCommandReferenceBlock(b, reference, request, operationsPerBlock, workload.fresh)
					candidateDuration += benchmarkRoaringBitmapCommandCandidateBlock(b, candidate, request, operationsPerBlock, workload.fresh)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func benchmarkRoaringBitmapCommandCandidateBlock(b *testing.B, trie *HatTrie, request CacheCommandRequest, operations int, fresh bool) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if fresh {
			if err := trie.UpsertRoaringBitmapChecked(request.Key); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkRoaringBitmapCommandResponseSink = trie.executeRoaringBitmapAddCommandValues(request.Key, request.Value, request.Values)
		if !benchmarkRoaringBitmapCommandResponseSink.OK {
			b.Fatal(benchmarkRoaringBitmapCommandResponseSink.Message)
		}
	}
	return time.Since(start)
}

func benchmarkRoaringBitmapCommandReferenceBlock(b *testing.B, trie *HatTrie, request CacheCommandRequest, operations int, fresh bool) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if fresh {
			if err := trie.UpsertRoaringBitmapChecked(request.Key); err != nil {
				b.Fatal(err)
			}
		}
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			b.Fatal(err)
		}
		added, err := trie.AddRoaringBitmapChecked(request.Key, values[0], values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkRoaringBitmapCommandResponseSink = CacheCommandResponse{OK: true, Message: "added roaring bitmap values", Value: strconv.Itoa(added)}
	}
	return time.Since(start)
}
