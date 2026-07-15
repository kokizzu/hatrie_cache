package hatriecache

import (
	"strconv"
	"testing"
)

var benchmarkCommandResponseSink CacheCommandResponse

func BenchmarkCommandFeature(b *testing.B) {
	benchmarks := []struct {
		name  string
		setup func(*testing.B, *HatTrie)
		run   func(*testing.B, *HatTrie, int)
	}{
		{name: "StringSet", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "SETSTR", Key: "string:key", Value: "value"})
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
			ttl := int64(3600)
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "EXPIRE", Key: "ttl:key", TTLSeconds: &ttl})
		}},
		{name: "MapPut", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}},
		{name: "MapPeek", setup: func(b *testing.B, ht *HatTrie) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}, run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PEEKMAP", Key: "map:key", Subkey: "field"})
		}},
		{name: "SlicePushPop", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice:key", Value: "value"})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "POPSLICE", Key: "slice:key"})
		}},
		{name: "SetAddHas", run: func(b *testing.B, ht *HatTrie, i int) {
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDSET", Key: "set:key", Value: "value"})
			benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "HASSET", Key: "set:key", Value: "value"})
		}},
		{name: "PriorityQueuePushPop", run: func(b *testing.B, ht *HatTrie, i int) {
			priority := int64(10)
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
			setupCommandFeatureXorStaged(b, buildTrie)
			benchmarkExecuteCommand(b, buildTrie, CacheCommandRequest{Command: "BUILDXF", Key: "xor:key"})
			buildTrie.Destroy()
		}},
		{name: "XorHas", setup: setupCommandFeatureXorBuilt, run: func(b *testing.B, ht *HatTrie, i int) {
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

func benchmarkExecuteCommand(b *testing.B, ht *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	b.Helper()
	response := ht.ExecuteCommand(request)
	if !response.OK {
		b.Fatalf("ExecuteCommand(%s, %s) = %#v, want ok", request.Command, request.Key, response)
	}
	benchmarkCommandResponseSink = response
	return response
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

func setupCommandFeatureXorStaged(b *testing.B, ht *HatTrie) {
	benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATEXF", Key: "xor:key", Value: "64"})
	for i := 0; i < 64; i++ {
		benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDXF", Key: "xor:key", Value: "value-" + strconv.Itoa(i)})
	}
}

func setupCommandFeatureXorBuilt(b *testing.B, ht *HatTrie) {
	setupCommandFeatureXorStaged(b, ht)
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
