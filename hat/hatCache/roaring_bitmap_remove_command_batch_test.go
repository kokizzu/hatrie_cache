package hatCache

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRoaringBitmapRemoveBatchCommandExactMatchesGeneric(t *testing.T) {
	plain, _ := benchmarkRoaringBitmapRemoveCommandValues(128)
	for _, test := range []struct {
		name    string
		command string
		setup   func(*testing.T, *HatTrie)
		request CacheCommandRequest
	}{
		{
			name:    "ExistingUint32",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: plain[:64]},
		},
		{
			name:    "Alias",
			command: "RBREM",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: plain[:8]},
		},
		{
			name:    "DeleteAlias",
			command: "DELRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: plain[:2]},
		},
		{
			name:    "LargeBatch",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: plain},
		},
		{
			name:    "MixedNumericRepresentations",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: Slice{
				uint32(1000), uint64(1001), uint(1002), int(1003), int32(1004), int64(1005),
				float64(1006), json.Number("1007"), " 1008 ",
			}},
		},
		{
			name:    "ScalarValue",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Value: " 1000 "},
		},
		{
			name:    "MissingBitmap",
			command: "REMRB",
			request: CacheCommandRequest{Values: plain[:8]},
		},
		{
			name:    "InvalidTail",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: Slice{uint32(1000), "-1"}},
		},
		{
			name:    "OverflowTail",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: Slice{uint32(1000), uint64(math.MaxUint32) + 1}},
		},
		{
			name:    "UnsupportedTail",
			command: "REMRB",
			setup:   setupRoaringBitmapRemoveCommandBatch128,
			request: CacheCommandRequest{Values: Slice{uint32(1000), Map{"value": 1001}}},
		},
		{name: "Empty", command: "REMRB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exactRequest := test.request
			exactRequest.Command = test.command
			exactResponse, exactState := runRoaringBitmapRemoveBatchCommandFixture(t, test.setup, exactRequest)

			genericRequest := test.request
			genericRequest.Command = " " + test.command
			genericResponse, genericState := runRoaringBitmapRemoveBatchCommandFixture(t, test.setup, genericRequest)
			if !reflect.DeepEqual(exactResponse, genericResponse) {
				t.Fatalf("exact response = %#v, generic = %#v", exactResponse, genericResponse)
			}
			if !reflect.DeepEqual(exactState, genericState) {
				t.Fatalf("exact state = %#v, generic = %#v", exactState, genericState)
			}
		})
	}
}

func TestRoaringBitmapRemoveBatchCommandPreservesWrongTypeAndTTL(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("roaring:batch", "old")
	if !trie.Expire("roaring:batch", time.Hour) {
		t.Fatal("Expire(roaring:batch) = false, want true")
	}
	beforeTTL, err := trie.TTLChecked("roaring:batch")
	if err != nil {
		t.Fatal(err)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{
		Command: "REMRB",
		Key:     "roaring:batch",
		Values:  Slice{uint32(1000), uint32(1001)},
	})
	if !response.OK || response.Value != "0" {
		t.Fatalf("REMRB wrong-type response = %#v, want removed 0", response)
	}
	if value, ok, err := trie.GetStringChecked("roaring:batch"); err != nil || !ok || value != "old" {
		t.Fatalf("GetStringChecked(roaring:batch) = %q/%v/%v, want old/true/nil", value, ok, err)
	}
	afterTTL, err := trie.TTLChecked("roaring:batch")
	if err != nil || afterTTL <= 0 || afterTTL > beforeTTL {
		t.Fatalf("TTLChecked before/after = %v/%v (%v), want preserved positive TTL", beforeTTL, afterTTL, err)
	}
}

func TestRoaringBitmapRemoveBatchCommandMatchesPublicAPI(t *testing.T) {
	values := []uint32{1000, 1001, 65535, 65536, 65543, math.MaxUint32}
	requestValues := make(Slice, len(values))
	for index, value := range values {
		requestValues[index] = value
	}

	commandTrie := newTestTrie(t)
	publicTrie := newTestTrie(t)
	setupRoaringBitmapRemoveCommandValues(t, commandTrie, values)
	setupRoaringBitmapRemoveCommandValues(t, publicTrie, values)

	response := commandTrie.ExecuteCommand(CacheCommandRequest{Command: "REMRB", Key: "roaring:batch", Values: requestValues})
	if !response.OK {
		t.Fatalf("REMRB response = %#v, want ok", response)
	}
	wantRemoved, err := publicTrie.RemoveRoaringBitmapChecked("roaring:batch", values[0], values[1:]...)
	if err != nil {
		t.Fatal(err)
	}
	wantResponse := CacheCommandResponse{OK: true, Message: "removed roaring bitmap values", Value: strconv.Itoa(wantRemoved)}
	if !reflect.DeepEqual(response, wantResponse) {
		t.Fatalf("command response = %#v, public response = %#v", response, wantResponse)
	}
	if got, want := roaringBitmapCommandState(t, commandTrie), roaringBitmapCommandState(t, publicTrie); !reflect.DeepEqual(got, want) {
		t.Fatalf("command state = %#v, public state = %#v", got, want)
	}
}

func TestRoaringBitmapRemoveBatchCommandSupportsLocalPartitions(t *testing.T) {
	values, typed := benchmarkRoaringBitmapRemoveCommandValues(64)
	single := newTestTrie(t)
	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	setupRoaringBitmapRemoveCommandValues(t, single, typed)
	setupRoaringBitmapRemoveCommandValues(t, partitioned, typed)

	request := CacheCommandRequest{Command: "REMRB", Key: "roaring:batch", Values: values}
	want := single.ExecuteCommand(request)
	got := partitioned.ExecuteCommand(request)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned response = %#v, single = %#v", got, want)
	}
	gotValues, gotOK, gotErr := partitioned.GetRoaringBitmapChecked("roaring:batch")
	wantValues, wantOK, wantErr := single.GetRoaringBitmapChecked("roaring:batch")
	if !reflect.DeepEqual(gotValues, wantValues) || gotOK != wantOK || errorString(gotErr) != errorString(wantErr) {
		t.Fatalf("GetRoaringBitmapChecked partitioned = %#v/%v/%v, single = %#v/%v/%v", gotValues, gotOK, gotErr, wantValues, wantOK, wantErr)
	}
}

func TestRoaringBitmapRemoveBatchCommandAllocationBudget(t *testing.T) {
	values, typed := benchmarkRoaringBitmapRemoveCommandValues(64)
	_, setupValues := benchmarkRoaringBitmapRemoveCommandValues(128)
	for _, test := range []struct {
		name    string
		request CacheCommandRequest
		restore []uint32
	}{
		{
			name:    "ExactBatch64",
			request: CacheCommandRequest{Command: "REMRB", Key: "roaring:batch", Values: values},
			restore: typed,
		},
		{
			name:    "AliasBatch64",
			request: CacheCommandRequest{Command: "RBDEL", Key: "roaring:batch", Values: values},
			restore: typed,
		},
		{
			name:    "Scalar",
			request: CacheCommandRequest{Command: "REMRB", Key: "roaring:batch", Value: "1000"},
			restore: typed[:1],
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			trie := newTestTrie(t)
			setupRoaringBitmapRemoveCommandValues(t, trie, setupValues)
			allocs := testing.AllocsPerRun(1000, func() {
				if _, err := trie.AddRoaringBitmapChecked("roaring:batch", test.restore[0], test.restore[1:]...); err != nil {
					panic(err)
				}
				if response := trie.ExecuteCommand(test.request); !response.OK {
					panic(response.Message)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocations = %.2f, want 0", allocs)
			}
		})
	}
}

func setupRoaringBitmapRemoveCommandBatch128(t *testing.T, trie *HatTrie) {
	t.Helper()
	_, values := benchmarkRoaringBitmapRemoveCommandValues(128)
	setupRoaringBitmapRemoveCommandValues(t, trie, values)
}

func setupRoaringBitmapRemoveCommandValues(t testing.TB, trie *HatTrie, values []uint32) {
	t.Helper()
	if err := trie.UpsertRoaringBitmapChecked("roaring:batch"); err != nil {
		t.Fatal(err)
	}
	if _, err := trie.AddRoaringBitmapChecked("roaring:batch", 1); err != nil {
		t.Fatal(err)
	}
	if len(values) > 0 {
		if _, err := trie.AddRoaringBitmapChecked("roaring:batch", values[0], values[1:]...); err != nil {
			t.Fatal(err)
		}
	}
}

func runRoaringBitmapRemoveBatchCommandFixture(t *testing.T, setup func(*testing.T, *HatTrie), request CacheCommandRequest) (CacheCommandResponse, roaringBitmapCommandBatchState) {
	t.Helper()
	trie := newTestTrie(t)
	if setup != nil {
		setup(t, trie)
	}
	request.Key = "roaring:batch"
	response := trie.ExecuteCommand(request)
	return response, roaringBitmapCommandState(t, trie)
}

func benchmarkRoaringBitmapRemoveCommandValues(count int) (Slice, []uint32) {
	values := make(Slice, count)
	typed := make([]uint32, count)
	for index := range values {
		value := uint32(1000 + index)
		values[index] = value
		typed[index] = value
	}
	return values, typed
}

func BenchmarkRoaringBitmapExistingRemoveBatchCommandPath(b *testing.B) {
	plain, typed := benchmarkRoaringBitmapRemoveCommandValues(128)
	mixed := make(Slice, 64)
	for index := range mixed {
		value := typed[index]
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
		name    string
		values  Slice
		restore []uint32
	}{
		{name: "Uint32_1", values: plain[:1], restore: typed[:1]},
		{name: "Uint32_2", values: plain[:2], restore: typed[:2]},
		{name: "Uint32_8", values: plain[:8], restore: typed[:8]},
		{name: "Uint32_64", values: plain[:64], restore: typed[:64]},
		{name: "Uint32_128", values: plain, restore: typed},
		{name: "Mixed64", values: mixed, restore: typed[:64]},
	} {
		b.Run(workload.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupRoaringBitmapRemoveCommandValues(b, trie, typed)
			request := CacheCommandRequest{Command: "REMRB", Key: "roaring:batch", Values: workload.values}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := trie.AddRoaringBitmapChecked("roaring:batch", workload.restore[0], workload.restore[1:]...); err != nil {
					b.Fatal(err)
				}
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}

	for _, command := range []struct {
		name    string
		command string
	}{
		{name: "ExactScalarValue", command: "REMRB"},
		{name: "GenericScalarValue", command: " REMRB"},
	} {
		b.Run(command.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			setupRoaringBitmapRemoveCommandValues(b, trie, typed[:1])
			request := CacheCommandRequest{Command: command.command, Key: "roaring:batch", Value: "1000"}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := trie.AddRoaringBitmapChecked("roaring:batch", typed[0]); err != nil {
					b.Fatal(err)
				}
				benchmarkExecuteCommand(b, trie, request)
			}
		})
	}
}
