package hatriecache

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

var benchmarkCommandCanonicalJSONStringResponseSink CacheCommandResponse

func TestCommandFastCanonicalJSONStringMatchesJSONMarshal(t *testing.T) {
	for valueByte := 0; valueByte <= 0xff; valueByte++ {
		value := string([]byte{byte(valueByte)})
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		want := string(encoded) == `"`+value+`"`
		if got := commandFastCanonicalJSONString(value); got != want {
			t.Fatalf("byte %#x canonical fast-path eligibility = %v, want %v from encoding/json output %q", valueByte, got, want, encoded)
		}
	}
}

func TestCommandCanonicalJSONStringFastPathsMatchGeneric(t *testing.T) {
	const value = "<script>&value>"
	tests := []struct {
		name    string
		command string
		subkey  string
	}{
		{name: "BloomFilter", command: "ADDBF"},
		{name: "CuckooFilter", command: "ADDCF"},
		{name: "XorFilter", command: "ADDXF"},
		{name: "CountMinSketch", command: "INCRCMS", subkey: "3"},
		{name: "HyperLogLog", command: "ADDHLL"},
		{name: "TopK", command: "ADDTOPK", subkey: "3"},
		{name: "ReservoirSample", command: "ADDRS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			request := CacheCommandRequest{
				Command: test.command,
				Key:     "canonical-json",
				Value:   value,
				Subkey:  test.subkey,
			}
			if response := fast.ExecuteCommand(request); !response.OK {
				t.Fatalf("fast %s response = %#v, want ok", test.command, response)
			}
			request.Command = strings.ToLower(test.command)
			if response := generic.ExecuteCommand(request); !response.OK {
				t.Fatalf("generic %s response = %#v, want ok", test.command, response)
			}

			got := commandCanonicalJSONStringSnapshot(t, fast, test.command, request.Key)
			want := commandCanonicalJSONStringSnapshot(t, generic, test.command, request.Key)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s fast snapshot differs from canonical generic snapshot\nfast:    %#v\ngeneric: %#v", test.command, got, want)
			}
		})
	}
}

func commandCanonicalJSONStringSnapshot(t *testing.T, ht *HatTrie, command string, key string) interface{} {
	t.Helper()
	hval := ht.Get(key)
	switch command {
	case "ADDBF":
		return ht.bloomFilters.array[hval.Index].Snapshot()
	case "ADDCF":
		return ht.cuckooFilters.array[hval.Index].Snapshot()
	case "ADDXF":
		return ht.xorFilters.array[hval.Index].Snapshot()
	case "INCRCMS":
		return ht.countMinSketches.array[hval.Index].Snapshot()
	case "ADDHLL":
		return ht.hyperLogLogs.array[hval.Index].Snapshot()
	case "ADDTOPK":
		return ht.topKs.array[hval.Index].Snapshot()
	case "ADDRS":
		return ht.reservoirSamples.array[hval.Index].Snapshot()
	default:
		t.Fatalf("unsupported snapshot command %q", command)
		return nil
	}
}

func BenchmarkCommandCanonicalJSONString(b *testing.B) {
	benchmarks := []struct {
		name    string
		prepare func(*testing.B, *HatTrie) CacheCommandRequest
	}{
		{
			name: "BloomHas",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "ADDBF", Key: "bloom", Value: "ordinary-value"})
				return CacheCommandRequest{Command: "HASBF", Key: "bloom", Value: "ordinary-value"}
			},
		},
		{
			name: "CuckooHas",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo", Value: "ordinary-value"})
				return CacheCommandRequest{Command: "HASCF", Key: "cuckoo", Value: "ordinary-value"}
			},
		},
		{
			name: "XorHas",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "CREATEXF", Key: "xor", Value: "1"})
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "ADDXF", Key: "xor", Value: "ordinary-value"})
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "BUILDXF", Key: "xor"})
				return CacheCommandRequest{Command: "HASXF", Key: "xor", Value: "ordinary-value"}
			},
		},
		{
			name: "CountMinEstimate",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "INCRCMS", Key: "cms", Value: "ordinary-value", Subkey: "1"})
				return CacheCommandRequest{Command: "ESTCMS", Key: "cms", Value: "ordinary-value"}
			},
		},
		{
			name: "HyperLogLogDuplicateAdd",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				request := CacheCommandRequest{Command: "ADDHLL", Key: "hll", Value: "ordinary-value"}
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, request)
				return request
			},
		},
		{
			name: "TopKDuplicateAdd",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				request := CacheCommandRequest{Command: "ADDTOPK", Key: "topk", Value: "key", Subkey: "1"}
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, request)
				return request
			},
		},
		{
			name: "ReservoirAdd",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				return CacheCommandRequest{Command: "ADDRS", Key: "reservoir", Value: "ordinary-value"}
			},
		},
		{
			name: "SetHasControl",
			prepare: func(b *testing.B, ht *HatTrie) CacheCommandRequest {
				mustExecuteCanonicalJSONStringBenchmarkCommand(b, ht, CacheCommandRequest{Command: "ADDSET", Key: "set", Value: "ordinary-value"})
				return CacheCommandRequest{Command: "HASSET", Key: "set", Value: "ordinary-value"}
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			request := benchmark.prepare(b, ht)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkCommandCanonicalJSONStringResponseSink = ht.ExecuteCommand(request)
			}
		})
	}
}

func mustExecuteCanonicalJSONStringBenchmarkCommand(b *testing.B, ht *HatTrie, request CacheCommandRequest) {
	b.Helper()
	if response := ht.ExecuteCommand(request); !response.OK {
		b.Fatalf("%s response = %#v, want ok", request.Command, response)
	}
}
