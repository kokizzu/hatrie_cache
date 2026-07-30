package hatriecache

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type quantileSketchCommandBatchState struct {
	present  bool
	hasTTL   bool
	writes   uint64
	snapshot quantileSketchSnapshot
	info     QuantileSketchInfo
}

func TestQuantileSketchBatchCommandExactMatchesGeneric(t *testing.T) {
	plain := benchmarkQuantileSketchCommandValues(64)
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		request CacheCommandRequest
	}{
		{name: "FreshFloat64", command: "ADDQ", request: CacheCommandRequest{Values: plain}},
		{name: "Alias", command: "QSADD", request: CacheCommandRequest{Values: plain[:8]}},
		{
			name:    "Existing",
			command: "ADDQ",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				setupQuantileSketchCommandBatch(t, trie, DefaultQuantileSketchEpsilon)
			},
			request: CacheCommandRequest{Values: plain},
		},
		{
			name:    "ReplaceExpiringString",
			command: "ADDQ",
			setup: func(t *testing.T, trie *HatTrie) {
				t.Helper()
				trie.UpsertString("quantile:batch", "old")
				if !trie.Expire("quantile:batch", time.Hour) {
					t.Fatal("Expire(quantile:batch) = false, want true")
				}
			},
			request: CacheCommandRequest{Values: plain},
		},
		{
			name:    "MixedNumericRepresentations",
			command: "QADD",
			request: CacheCommandRequest{Values: Slice{
				float64(1.25), float32(2.5), int(3), int32(4), int64(5),
				uint(6), uint32(7), uint64(8), json.Number("9.5"), " 10.25 ",
			}},
		},
		{name: "ScalarValue", command: "ADDQ", request: CacheCommandRequest{Value: " 1.25 "}},
		{name: "InvalidTail", command: "ADDQ", setup: setupExistingQuantileSketchCommandBatch, request: CacheCommandRequest{Values: Slice{1.0, math.NaN()}}},
		{name: "InfiniteTail", command: "ADDQ", setup: setupExistingQuantileSketchCommandBatch, request: CacheCommandRequest{Values: Slice{1.0, math.Inf(1)}}},
		{name: "UnsupportedTail", command: "ADDQ", setup: setupExistingQuantileSketchCommandBatch, request: CacheCommandRequest{Values: Slice{1.0, Map{"value": 2}}}},
		{name: "InvalidFresh", command: "ADDQ", request: CacheCommandRequest{Values: Slice{1.0, func() {}}}},
		{name: "Empty", command: "ADDQ"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactRequest := test.request
			exactRequest.Command = test.command
			exactResponse, exactState := runQuantileSketchBatchCommandFixture(t, test.setup, exactRequest)

			genericRequest := test.request
			genericRequest.Command = " " + test.command
			genericResponse, genericState := runQuantileSketchBatchCommandFixture(t, test.setup, genericRequest)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func TestQuantileSketchBatchCommandMatchesPublicAPI(t *testing.T) {
	values := []float64{-100, -1.25, 0, 1.5, 9, 1000}
	requestValues := make(Slice, len(values))
	for index, value := range values {
		requestValues[index] = value
	}

	commandTrie := newTestTrie(t)
	publicTrie := newTestTrie(t)
	setupQuantileSketchCommandBatch(t, commandTrie, DefaultQuantileSketchEpsilon)
	setupQuantileSketchCommandBatch(t, publicTrie, DefaultQuantileSketchEpsilon)

	response := commandTrie.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "quantile:batch", Values: requestValues})
	if !response.OK {
		t.Fatalf("ADDQ response = %#v, want ok", response)
	}
	wantEstimate, err := publicTrie.AddQuantileSketchChecked("quantile:batch", values[0], values[1:]...)
	if err != nil {
		t.Fatal(err)
	}
	wantResponse := commandValueResponse("added quantile sketch values", wantEstimate)
	if !reflect.DeepEqual(response, wantResponse) {
		t.Fatalf("command response = %#v, public response = %#v", response, wantResponse)
	}

	_, commandState := runQuantileSketchBatchCommandFixture(t, func(t *testing.T, trie *HatTrie) {
		t.Helper()
		setupQuantileSketchCommandBatch(t, trie, DefaultQuantileSketchEpsilon)
	}, CacheCommandRequest{Command: "ADDQ", Key: "quantile:batch", Values: requestValues})
	publicState := quantileSketchCommandState(t, publicTrie)
	if !reflect.DeepEqual(commandState.snapshot, publicState.snapshot) || !reflect.DeepEqual(commandState.info, publicState.info) {
		t.Fatalf("command state = %#v, public state = %#v", commandState, publicState)
	}
}

func TestQuantileSketchBatchCommandSupportsLocalPartitions(t *testing.T) {
	values := benchmarkQuantileSketchCommandValues(64)
	single := newTestTrie(t)
	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}

	want := single.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "quantile:partitioned", Values: values})
	got := partitioned.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "quantile:partitioned", Values: values})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned response = %#v, single = %#v", got, want)
	}
	for _, quantile := range []float64{0, 0.5, 1} {
		gotEstimate, gotOK, gotErr := partitioned.EstimateQuantileSketchChecked("quantile:partitioned", quantile)
		wantEstimate, wantOK, wantErr := single.EstimateQuantileSketchChecked("quantile:partitioned", quantile)
		if gotEstimate != wantEstimate || gotOK != wantOK || errorString(gotErr) != errorString(wantErr) {
			t.Fatalf("EstimateQuantileSketchChecked(%v) partitioned = %#v/%v/%v, single = %#v/%v/%v", quantile, gotEstimate, gotOK, gotErr, wantEstimate, wantOK, wantErr)
		}
	}
}

func setupExistingQuantileSketchCommandBatch(t *testing.T, trie *HatTrie) {
	t.Helper()
	setupQuantileSketchCommandBatch(t, trie, DefaultQuantileSketchEpsilon)
}

func setupQuantileSketchCommandBatch(t testing.TB, trie *HatTrie, epsilon float64) {
	t.Helper()
	if err := trie.UpsertQuantileSketch("quantile:batch", epsilon); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.AddQuantileSketchChecked("quantile:batch", -10, -1, 0, 1, 10); err != nil {
		t.Fatal(err)
	}
}

func runQuantileSketchBatchCommandFixture(t *testing.T, setup func(*testing.T, *HatTrie), request CacheCommandRequest) (CacheCommandResponse, quantileSketchCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	request.Key = "quantile:batch"
	response := trie.ExecuteCommand(request)
	return response, quantileSketchCommandState(t, trie)
}

func quantileSketchCommandState(t testing.TB, trie *HatTrie) quantileSketchCommandBatchState {
	t.Helper()
	stats := trie.Stats()
	trie.mu.RLock()
	defer trie.mu.RUnlock()
	raw := trie.tryLocation("quantile:batch")
	if raw == nil {
		return quantileSketchCommandBatchState{writes: stats.Writes}
	}
	var value HatValue
	value.fromValue(*raw)
	if !value.IsQuantileSketch() {
		t.Fatalf("quantile:batch type = %d, want quantile sketch", value.Type())
	}
	data := trie.quantileSketches.array[value.Index]
	return quantileSketchCommandBatchState{
		present:  true,
		hasTTL:   value.HasTtl(),
		writes:   stats.Writes,
		snapshot: data.Snapshot(),
		info:     data.Info(),
	}
}

func benchmarkQuantileSketchCommandValues(count int) Slice {
	values := make(Slice, count)
	for index := range values {
		values[index] = float64((index*37)%count) + 0.25
	}
	return values
}

func BenchmarkQuantileSketchExistingBatchCommandPath(b *testing.B) {
	floatValues := benchmarkQuantileSketchCommandValues(128)
	stringValues := make(Slice, 64)
	numberValues := make(Slice, 64)
	mixedValues := make(Slice, 64)
	for index := range stringValues {
		text := strconv.FormatFloat(floatValues[index].(float64), 'f', -1, 64)
		stringValues[index] = text
		numberValues[index] = json.Number(text)
		switch index % 4 {
		case 0:
			mixedValues[index] = floatValues[index]
		case 1:
			mixedValues[index] = int64(index)
		case 2:
			mixedValues[index] = json.Number(text)
		default:
			mixedValues[index] = text
		}
	}

	for _, workload := range []struct {
		name   string
		values Slice
	}{
		{name: "Float64_1", values: floatValues[:1]},
		{name: "Float64_2", values: floatValues[:2]},
		{name: "Float64_8", values: floatValues[:8]},
		{name: "Float64_64", values: floatValues[:64]},
		{name: "Float64_128", values: floatValues},
		{name: "String64", values: stringValues},
		{name: "JSONNumber64", values: numberValues},
		{name: "Mixed64", values: mixedValues},
	} {
		b.Run(workload.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupQuantileSketchCommandBatch(b, trie, 0.5)
			request := CacheCommandRequest{Command: "ADDQ", Key: "quantile:batch", Values: workload.values}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}

	for _, command := range []struct {
		name  string
		value string
	}{
		{name: "ExactScalarValue", value: "ADDQ"},
		{name: "GenericScalarValue", value: " ADDQ"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupQuantileSketchCommandBatch(b, trie, 0.5)
			request := CacheCommandRequest{Command: command.value, Key: "quantile:batch", Value: "1.25"}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}

func BenchmarkQuantileSketchFreshBatchCommandPath(b *testing.B) {
	values := benchmarkQuantileSketchCommandValues(64)
	trie := CreateHatTrie()
	defer trie.Destroy()
	request := CacheCommandRequest{Command: "ADDQ", Key: "quantile:batch", Values: values}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := trie.UpsertQuantileSketch("quantile:batch", 0.5); err != nil {
			b.Fatal(err)
		}
		benchmarkExecuteCommand(b, trie, request)
	}
}

var benchmarkQuantileSketchCommandResponseSink CacheCommandResponse

func BenchmarkQuantileSketchCommandBatchAlternating(b *testing.B) {
	floatValues := benchmarkQuantileSketchCommandValues(128)
	stringValues := make(Slice, 64)
	numberValues := make(Slice, 64)
	mixedValues := make(Slice, 64)
	for index := range stringValues {
		text := strconv.FormatFloat(floatValues[index].(float64), 'f', -1, 64)
		stringValues[index] = text
		numberValues[index] = json.Number(text)
		switch index % 4 {
		case 0:
			mixedValues[index] = floatValues[index]
		case 1:
			mixedValues[index] = int64(index)
		case 2:
			mixedValues[index] = json.Number(text)
		default:
			mixedValues[index] = text
		}
	}

	for _, workload := range []struct {
		name   string
		values Slice
		fresh  bool
	}{
		{name: "Float64_1", values: floatValues[:1]},
		{name: "Float64_2", values: floatValues[:2]},
		{name: "Float64_8", values: floatValues[:8]},
		{name: "Float64_64", values: floatValues[:64]},
		{name: "Float64_128", values: floatValues},
		{name: "String64", values: stringValues},
		{name: "JSONNumber64", values: numberValues},
		{name: "Mixed64", values: mixedValues},
		{name: "FreshFloat64_64", values: floatValues[:64], fresh: true},
	} {
		b.Run(workload.name, func(b *testing.B) {
			candidate := CreateHatTrie()
			defer candidate.Destroy()
			reference := CreateHatTrie()
			defer reference.Destroy()
			setupQuantileSketchCommandBatch(b, candidate, 0.5)
			setupQuantileSketchCommandBatch(b, reference, 0.5)
			request := CacheCommandRequest{Command: "ADDQ", Key: "quantile:batch", Values: workload.values}

			const operationsPerBlock = 64
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkQuantileSketchCommandCandidateBlock(b, candidate, request, operationsPerBlock, workload.fresh)
					referenceDuration += benchmarkQuantileSketchCommandReferenceBlock(b, reference, request, operationsPerBlock, workload.fresh)
				} else {
					referenceDuration += benchmarkQuantileSketchCommandReferenceBlock(b, reference, request, operationsPerBlock, workload.fresh)
					candidateDuration += benchmarkQuantileSketchCommandCandidateBlock(b, candidate, request, operationsPerBlock, workload.fresh)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func benchmarkQuantileSketchCommandCandidateBlock(b *testing.B, trie *HatTrie, request CacheCommandRequest, operations int, fresh bool) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if fresh {
			if err := trie.UpsertQuantileSketch(request.Key, 0.5); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkQuantileSketchCommandResponseSink = trie.executeQuantileSketchCommandValues(request.Key, request.Value, request.Values)
		if !benchmarkQuantileSketchCommandResponseSink.OK {
			b.Fatal(benchmarkQuantileSketchCommandResponseSink.Message)
		}
	}
	return time.Since(start)
}

func benchmarkQuantileSketchCommandReferenceBlock(b *testing.B, trie *HatTrie, request CacheCommandRequest, operations int, fresh bool) time.Duration {
	b.Helper()
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if fresh {
			if err := trie.UpsertQuantileSketch(request.Key, 0.5); err != nil {
				b.Fatal(err)
			}
		}
		values, err := quantileSketchValuesFromCommand(request)
		if err != nil {
			b.Fatal(err)
		}
		estimate, err := trie.AddQuantileSketchChecked(request.Key, values[0], values[1:]...)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkQuantileSketchCommandResponseSink = commandValueResponse("added quantile sketch values", estimate)
	}
	return time.Since(start)
}
