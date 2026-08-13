package hatriecache

import (
	"strconv"
	"testing"
)

var benchmarkCommandResponseSink CacheCommandResponse
var benchmarkCommandFastInt64Sink int64
var benchmarkCommandFastFloat64Sink float64
var benchmarkCommandFastUint64Sink uint64
var benchmarkBloomConfigSink uint64

const benchmarkCommandPipelineOps = 16
const benchmarkCommandMixedProfileOps = 100
const benchmarkCommandMixedKeyspace = 100

func BenchmarkCommandFeature(b *testing.B) {
	pipelineBatch := make([]CacheCommandRequest, 0, benchmarkCommandPipelineOps)
	for i := 0; i < benchmarkCommandPipelineOps; i++ {
		idx := strconv.Itoa(i)
		pipelineBatch = append(pipelineBatch, CacheCommandRequest{Command: "SETSTR", Key: "pipeline:string:" + idx, Value: "value"})
	}
	xorValues := benchmarkXorCommandValues()
	mixedReadHeavy := benchmarkCommandMixedReadHeavyRequests()
	mixedWriteHeavy := benchmarkCommandMixedWriteHeavyRequests()
	ttlSeconds := int64(3600)
	priority := int64(10)
	benchmarks := []struct {
		name  string
		setup func(*testing.B, *HatTrie)
		run   func(*testing.B, *HatTrie, int)
	}{
		{name: "StringSet", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: "string:key", Value: "value"})
		}},
		{name: "PipelineBatch16", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "BATCH", Batch: pipelineBatch})
		}},
		{name: "MixedReadHeavy100", setup: setupCommandFeatureMixedProfile, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommandProfile(b, ht, mixedReadHeavy)
		}},
		{name: "MixedWriteHeavy100", setup: setupCommandFeatureMixedProfile, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommandProfile(b, ht, mixedWriteHeavy)
		}},
		{name: "StringGet", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: "string:key", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GET", Key: "string:key"})
		}},
		{name: "CounterInc", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETINT", Key: "counter:key", Value: "0"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "INC", Key: "counter:key", Value: "1"})
		}},
		{name: "TTLExpire", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: "ttl:key", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "EXPIRE", Key: "ttl:key", TTLSeconds: &ttlSeconds})
		}},
		{name: "MapPut", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}},
		{name: "MapPeek", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PEEKMAP", Key: "map:key", Subkey: "field"})
		}},
		{name: "MapGet", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GET", Key: "map:key"})
		}},
		{name: "SlicePushPop", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice:key", Value: "value"})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "POPSLICE", Key: "slice:key"})
		}},
		{name: "SliceGet", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice:key", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GET", Key: "slice:key"})
		}},
		{name: "SetAddHas", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDSET", Key: "set:key", Value: "value"})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASSET", Key: "set:key", Value: "value"})
		}},
		{name: "SetGet", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDSET", Key: "set:key", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GET", Key: "set:key"})
		}},
		{name: "PriorityQueuePushPop", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUSHPQ", Key: "priority:key", Value: "value", Priority: &priority})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "POPPQ", Key: "priority:key"})
		}},
		{name: "BloomAdd", setup: setupCommandFeatureBloom, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDBF", Key: "bloom:key", Value: "value"})
		}},
		{name: "BloomHas", setup: setupCommandFeatureBloomWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASBF", Key: "bloom:key", Value: "value"})
		}},
		{name: "CuckooDeleteAdd", setup: setupCommandFeatureCuckooWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "DELCF", Key: "cuckoo:key", Value: "value"})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:key", Value: "value"})
		}},
		{name: "CuckooHas", setup: setupCommandFeatureCuckooWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASCF", Key: "cuckoo:key", Value: "value"})
		}},
		{name: "XorBuild64Items", run: func(b *testing.B, ht *HatTrie, i int) {
			buildTrie := CreateHatTrie()
			setupCommandFeatureXorStaged(b, buildTrie, xorValues)
			benchmarkExecuteCommand(b, buildTrie, CacheCommandRequest{Command: "BUILDXF", Key: "xor:key"})
			buildTrie.Destroy()
		}},
		{name: "XorHas", setup: func(b *testing.B, ht *HatTrie) {
			setupCommandFeatureXorBuilt(b, ht, xorValues)
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASXF", Key: "xor:key", Value: "value-7"})
		}},
		{name: "RoaringAdd", setup: setupCommandFeatureRoaring, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDRB", Key: "roaring:key", Value: "65543"})
		}},
		{name: "RoaringHas", setup: setupCommandFeatureRoaringWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASRB", Key: "roaring:key", Value: "65543"})
		}},
		{name: "SparseBitsetAdd", setup: setupCommandFeatureSparseBitset, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDSB", Key: "sparse:key", Value: "18446744073709551615"})
		}},
		{name: "SparseBitsetHas", setup: setupCommandFeatureSparseBitsetWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASSB", Key: "sparse:key", Value: "18446744073709551615"})
		}},
		{name: "RadixPut", setup: setupCommandFeatureRadix, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTRT", Key: "radix:key", Subkey: "session:active", Value: "value"})
		}},
		{name: "RadixHas", setup: setupCommandFeatureRadixWithValues, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASRT", Key: "radix:key", Subkey: "session:1"})
		}},
		{name: "RadixPrefix", setup: setupCommandFeatureRadixWithValues, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PREFIXRT", Key: "radix:key", Subkey: "session:"})
		}},
		{name: "CountMinSketchIncrement", setup: setupCommandFeatureCountMinSketch, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "INCRCMS", Key: "cms:key", Value: "value", Subkey: "1"})
		}},
		{name: "CountMinSketchEstimate", setup: setupCommandFeatureCountMinSketchWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ESTCMS", Key: "cms:key", Value: "value"})
		}},
		{name: "HyperLogLogAdd", setup: setupCommandFeatureHyperLogLog, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDHLL", Key: "hll:key", Value: "value"})
		}},
		{name: "HyperLogLogCount", setup: setupCommandFeatureHyperLogLogWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "COUNTHLL", Key: "hll:key"})
		}},
		{name: "TopKAdd", setup: setupCommandFeatureTopK, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDTOPK", Key: "topk:key", Value: "value", Subkey: "1"})
		}},
		{name: "TopKGet", setup: setupCommandFeatureTopKWithValue, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GETTOPK", Key: "topk:key"})
		}},
		{name: "ReservoirSampleAdd", setup: setupCommandFeatureReservoirSample, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDRS", Key: "sample:key", Value: "value"})
		}},
		{name: "ReservoirSampleGet", setup: setupCommandFeatureReservoirSampleWithValues, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "GETRS", Key: "sample:key"})
		}},
		{name: "QuantileSketchAdd", setup: setupCommandFeatureQuantileSketch, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDQ", Key: "quantile:key", Value: "42.5"})
		}},
		{name: "QuantileSketchEstimate", setup: setupCommandFeatureQuantileSketchWithValues, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ESTQ", Key: "quantile:key", Value: "0.5"})
		}},
		{name: "FenwickTreeAdd", setup: setupCommandFeatureFenwickTree, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDFW", Key: "fenwick:key", Value: "32", Subkey: "1"})
		}},
		{name: "FenwickTreeRange", setup: setupCommandFeatureFenwickTreeWithValues, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "RANGEFW", Key: "fenwick:key", Value: "1", Subkey: "64"})
		}},
		{name: "ReplicationDump", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: "replication:key", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "DUMP", Key: "replication:key"})
		}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer func() {
				ht.Destroy()
			}()
			if benchmark.setup != nil {
				benchmark.setup(b, ht)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmark.run(b, ht, i)
			}
		})
	}
}

func BenchmarkCommandFastInt64Field(b *testing.B) {
	values := [...]string{"1", "-1", "+42", "-9223372036854775808", "9223372036854775807"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, ok := commandFastInt64Field(values[iteration%len(values)])
		if !ok {
			b.Fatal("commandFastInt64Field() = false, want valid value")
		}
		benchmarkCommandFastInt64Sink = value
	}
}

func BenchmarkCommandFastUint64Field(b *testing.B) {
	values := [...]string{"1", "42", "001", "18446744073709551615", "1844674407370955161"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, ok := commandFastUint64Field(values[iteration%len(values)])
		if !ok {
			b.Fatal("commandFastUint64Field() = false, want valid value")
		}
		benchmarkCommandFastUint64Sink = value
	}
}

func BenchmarkCommandFastUint64FieldShort(b *testing.B) {
	values := [...]string{"1", "42", "001", "65543", "1844674407370955161"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, ok := commandFastUint64Field(values[iteration%len(values)])
		if !ok {
			b.Fatal("commandFastUint64Field() = false, want valid value")
		}
		benchmarkCommandFastUint64Sink = value
	}
}

func BenchmarkCommandBloomFilterConfig(b *testing.B) {
	request := CacheCommandRequest{Value: "32768", Subkey: "0.001"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		expected, rate, err := commandBloomFilterConfig(request)
		if err != nil || expected != 32768 || rate != 0.001 {
			b.Fatalf("commandBloomFilterConfig() = %d/%v/%v, want 32768/0.001/nil", expected, rate, err)
		}
		benchmarkBloomConfigSink = expected
	}
}

func BenchmarkCommandPrioritySubkey(b *testing.B) {
	values := [...]string{"1", "-1", " +42\t", "-9223372036854775808", "9223372036854775807"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, ok := commandPrioritySubkey(values[iteration%len(values)])
		if !ok {
			b.Fatal("commandPrioritySubkey() = false, want valid value")
		}
		benchmarkCommandFastInt64Sink = value
	}
}

func BenchmarkCommandFastFloat64Field(b *testing.B) {
	values := [...]string{"1", "-1.25", "+42.5", "1e-9", "3.141592653589793"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value, ok := commandFastFloat64Field(values[iteration%len(values)])
		if !ok {
			b.Fatal("commandFastFloat64Field() = false, want valid value")
		}
		benchmarkCommandFastFloat64Sink = value
	}
}

func BenchmarkXorCommandBuild64Path(b *testing.B) {
	values := benchmarkXorCommandValues()
	for _, benchmark := range []struct {
		name       string
		addCommand string
	}{
		{name: "Generic", addCommand: " ADDXF"},
		{name: "PlainJSONString", addCommand: "ADDXF"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ht := CreateHatTrie()
				benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEXF", Key: "xor:key", Value: "64"})
				for _, value := range values {
					benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: benchmark.addCommand, Key: "xor:key", Value: value})
				}
				benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "BUILDXF", Key: "xor:key"})
				ht.Destroy()
			}
		})
	}
}

func benchmarkXorCommandValues() []string {
	values := make([]string, 64)
	for i := range values {
		values[i] = "value-" + strconv.Itoa(i)
	}
	return values
}

func BenchmarkReservoirSampleGetPath(b *testing.B) {
	for _, benchmark := range []struct {
		name       string
		getCommand string
	}{
		{name: "Generic", getCommand: " GETRS"},
		{name: "PlainJSONString", getCommand: "GETRS"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			setupCommandFeatureReservoirSampleWithValues(b, ht)
			request := CacheCommandRequest{Command: benchmark.getCommand, Key: "sample:key"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkExecuteCommand(b, ht, request)
			}
		})
	}
}

func BenchmarkReservoirSampleGetEncodedPath(b *testing.B) {
	for _, benchmark := range []struct {
		name       string
		getCommand string
	}{
		{name: "Generic", getCommand: " GETRS"},
		{name: "Exact", getCommand: "GETRS"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			if err := ht.UpsertReservoirSample("sample:key", 16); err != nil {
				b.Fatalf("UpsertReservoirSample() error = %v", err)
			}
			for i := 0; i < 16; i++ {
				value := "escaped-\"value-" + strconv.Itoa(i)
				if update := ht.AddReservoirSample("sample:key", value); !update.Accepted {
					b.Fatalf("AddReservoirSample(%q) = %#v, want accepted", value, update)
				}
			}
			request := CacheCommandRequest{Command: benchmark.getCommand, Key: "sample:key"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkExecuteCommand(b, ht, request)
			}
		})
	}
}

func BenchmarkReservoirSampleSmallGetCommand(b *testing.B) {
	for _, size := range []int{0, 1, 16} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			if err := ht.UpsertReservoirSample("sample:key", 16); err != nil {
				b.Fatalf("UpsertReservoirSample() error = %v", err)
			}
			for index := 0; index < size; index++ {
				value := "value-" + strconv.Itoa(index)
				if update := ht.AddReservoirSample("sample:key", value); !update.Accepted {
					b.Fatalf("AddReservoirSample(%q) = %#v, want accepted", value, update)
				}
			}
			request := CacheCommandRequest{Command: "GETRS", Key: "sample:key"}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkExecuteCommand(b, ht, request)
			}
		})
	}
}

func BenchmarkTopKGetPath(b *testing.B) {
	for _, size := range []int{16, 100} {
		for _, benchmark := range []struct {
			name       string
			getCommand string
		}{
			{name: "Generic", getCommand: " GETTOPK"},
			{name: "Exact", getCommand: "GETTOPK"},
		} {
			b.Run("Strings"+strconv.Itoa(size)+"/"+benchmark.name, func(b *testing.B) {
				ht := CreateHatTrie()
				defer ht.Destroy()
				setupCommandFeatureTopKWithValues(b, ht, size)
				request := CacheCommandRequest{Command: benchmark.getCommand, Key: "topk:key"}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchmarkExecuteCommand(b, ht, request)
				}
			})
		}
	}
}

func BenchmarkTopKGetStructuredFallbackPath(b *testing.B) {
	for _, benchmark := range []struct {
		name       string
		getCommand string
	}{
		{name: "Generic", getCommand: " GETTOPK"},
		{name: "Exact", getCommand: "GETTOPK"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			setupCommandFeatureTopKWithValues(b, ht, 15)
			if estimate, err := ht.AddTopKChecked("topk:key", Map{"route": "/api/cache"}, 100); err != nil || !estimate.Tracked {
				b.Fatalf("AddTopKChecked(map) = %#v/%v, want tracked", estimate, err)
			}
			request := CacheCommandRequest{Command: benchmark.getCommand, Key: "topk:key"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkExecuteCommand(b, ht, request)
			}
		})
	}
}

func benchmarkExecuteCommand(b *testing.B, ht *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	response := ht.ExecuteCommand(request)
	if !response.OK {
		b.Helper()
		b.Fatalf("ExecuteCommand(%s, %s) = %#v, want ok", request.Command, request.Key, response)
	}
	benchmarkCommandResponseSink = response
	return response
}

func benchmarkExecuteCommandProfile(b *testing.B, ht *HatTrie, requests []CacheCommandRequest) {
	for _, request := range requests {
		response := ht.ExecuteCommand(request)
		if !response.OK {
			b.Helper()
			b.Fatalf("ExecuteCommand(%s, %s) = %#v, want ok", request.Command, request.Key, response)
		}
		benchmarkCommandResponseSink = response
	}
}

func setupCommandFeatureMixedProfile(b *testing.B, ht *HatTrie) {
	for idx := 0; idx < benchmarkCommandMixedKeyspace; idx++ {
		key := "mixed:string:" + strconv.Itoa(idx)
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"})
	}
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETINT", Key: "mixed:counter", Value: "0"})
}

func benchmarkCommandMixedReadHeavyRequests() []CacheCommandRequest {
	requests := make([]CacheCommandRequest, 0, benchmarkCommandMixedProfileOps)
	for idx := 0; idx < 90; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "GET", Key: "mixed:string:" + strconv.Itoa(idx)})
	}
	for idx := 0; idx < 5; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "SETSTR", Key: "mixed:string:" + strconv.Itoa(idx), Value: "value"})
	}
	for idx := 0; idx < 4; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "EXISTS", Key: "mixed:string:" + strconv.Itoa(idx)})
	}
	requests = append(requests, CacheCommandRequest{Command: "INC", Key: "mixed:counter", Value: "1"})
	return requests
}

func benchmarkCommandMixedWriteHeavyRequests() []CacheCommandRequest {
	ttl := int64(3600)
	requests := make([]CacheCommandRequest, 0, benchmarkCommandMixedProfileOps)
	for idx := 0; idx < 40; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "SETSTR", Key: "mixed:string:" + strconv.Itoa(idx), Value: "value"})
	}
	for idx := 0; idx < 30; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "EXPIRE", Key: "mixed:string:" + strconv.Itoa(idx), TTLSeconds: &ttl})
	}
	for idx := 0; idx < 20; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "GET", Key: "mixed:string:" + strconv.Itoa(idx)})
	}
	for idx := 0; idx < 10; idx++ {
		requests = append(requests, CacheCommandRequest{Command: "INC", Key: "mixed:counter", Value: "1"})
	}
	return requests
}

func setupCommandFeatureBloom(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEBF", Key: "bloom:key", Value: "32768", Subkey: "0.001"})
}

func setupCommandFeatureBloomWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureBloom(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDBF", Key: "bloom:key", Value: "value"})
}

func setupCommandFeatureCuckooWithValue(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATECF", Key: "cuckoo:key", Value: "32768", Subkey: "0.001"})
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:key", Value: "value"})
}

func setupCommandFeatureXorStaged(b *testing.B, ht *HatTrie, values []string) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEXF", Key: "xor:key", Value: "64"})
	for _, value := range values {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDXF", Key: "xor:key", Value: value})
	}
}

func setupCommandFeatureXorBuilt(b *testing.B, ht *HatTrie, values []string) {
	setupCommandFeatureXorStaged(b, ht, values)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "BUILDXF", Key: "xor:key"})
}

func setupCommandFeatureRoaring(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATERB", Key: "roaring:key"})
}

func setupCommandFeatureRoaringWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureRoaring(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDRB", Key: "roaring:key", Value: "65543"})
}

func setupCommandFeatureSparseBitset(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATESB", Key: "sparse:key"})
}

func setupCommandFeatureSparseBitsetWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureSparseBitset(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDSB", Key: "sparse:key", Value: "18446744073709551615"})
}

func setupCommandFeatureRadix(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATERT", Key: "radix:key"})
}

func setupCommandFeatureRadixWithValues(b *testing.B, ht *HatTrie) {
	setupCommandFeatureRadix(b, ht)
	for i := 0; i < 16; i++ {
		idx := strconv.Itoa(i)
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTRT", Key: "radix:key", Subkey: "session:" + idx, Value: "value-" + idx})
	}
}

func setupCommandFeatureCountMinSketch(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATECMS", Key: "cms:key", Value: "1024", Subkey: "4"})
}

func setupCommandFeatureCountMinSketchWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureCountMinSketch(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "INCRCMS", Key: "cms:key", Value: "value", Subkey: "1"})
}

func setupCommandFeatureHyperLogLog(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEHLL", Key: "hll:key", Value: "10"})
}

func setupCommandFeatureHyperLogLogWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureHyperLogLog(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDHLL", Key: "hll:key", Value: "value"})
}

func setupCommandFeatureTopK(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATETOPK", Key: "topk:key", Value: "16"})
}

func setupCommandFeatureTopKWithValue(b *testing.B, ht *HatTrie) {
	setupCommandFeatureTopK(b, ht)
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDTOPK", Key: "topk:key", Value: "value", Subkey: "2"})
}

func setupCommandFeatureTopKWithValues(b *testing.B, ht *HatTrie, size int) {
	b.Helper()
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATETOPK", Key: "topk:key", Value: strconv.Itoa(size)})
	for idx := 0; idx < size; idx++ {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{
			Command: "ADDTOPK",
			Key:     "topk:key",
			Value:   "value-" + strconv.Itoa(idx),
			Subkey:  strconv.Itoa(idx + 1),
		})
	}
}

func setupCommandFeatureReservoirSample(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATERS", Key: "sample:key", Value: "16"})
}

func setupCommandFeatureReservoirSampleWithValues(b *testing.B, ht *HatTrie) {
	setupCommandFeatureReservoirSample(b, ht)
	for i := 0; i < 16; i++ {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDRS", Key: "sample:key", Value: "value-" + strconv.Itoa(i)})
	}
}

func setupCommandFeatureQuantileSketch(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEQ", Key: "quantile:key", Value: "0.01"})
}

func setupCommandFeatureQuantileSketchWithValues(b *testing.B, ht *HatTrie) {
	setupCommandFeatureQuantileSketch(b, ht)
	for i := 0; i < 32; i++ {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDQ", Key: "quantile:key", Value: strconv.Itoa(i)})
	}
}

func setupCommandFeatureFenwickTree(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEFW", Key: "fenwick:key", Value: "128"})
}

func setupCommandFeatureFenwickTreeWithValues(b *testing.B, ht *HatTrie) {
	setupCommandFeatureFenwickTree(b, ht)
	for i := 1; i <= 64; i++ {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDFW", Key: "fenwick:key", Value: strconv.Itoa(i), Subkey: "1"})
	}
}
