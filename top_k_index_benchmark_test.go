package hatriecache

import (
	"runtime"
	"strconv"
	"testing"
	"time"
)

const topKSmallIndexBenchmarkSketches = 100000

var topKSmallIndexBenchmarkSliceSink []topKData
var topKSmallIndexBenchmarkTopSink topKData
var topKSmallIndexBenchmarkEstimateSink TopKEstimate

func BenchmarkTopKSmallIndexMemory(b *testing.B) {
	for _, size := range []int{1, 2, 3} {
		values := topKSmallIndexBenchmarkValues(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			var retainedBytes uint64
			var retainedObjects uint64
			var buildTime time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				topKSmallIndexBenchmarkSliceSink = nil
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)

				started := time.Now()
				tops := make([]topKData, topKSmallIndexBenchmarkSketches)
				for idx := range tops {
					top := newDefaultTopKData()
					for valueIdx, value := range values {
						top.addPlainJSONString(value, uint64(valueIdx+1))
					}
					tops[idx] = top
				}
				buildTime += time.Since(started)
				topKSmallIndexBenchmarkSliceSink = tops
				runtime.GC()

				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				if after.HeapAlloc >= before.HeapAlloc {
					retainedBytes += after.HeapAlloc - before.HeapAlloc
				}
				if after.HeapObjects >= before.HeapObjects {
					retainedObjects += after.HeapObjects - before.HeapObjects
				}
			}
			b.ReportMetric(float64(retainedBytes)/float64(b.N*topKSmallIndexBenchmarkSketches), "retained_B/sketch")
			b.ReportMetric(float64(retainedObjects)/float64(b.N*topKSmallIndexBenchmarkSketches), "objects/sketch")
			b.ReportMetric(float64(buildTime.Nanoseconds())/float64(b.N*topKSmallIndexBenchmarkSketches), "build_ns/sketch")
			topKSmallIndexBenchmarkSliceSink = nil
			runtime.GC()
		})
	}
}

func BenchmarkTopKSmallIndexLifecycle(b *testing.B) {
	for _, size := range []int{1, 2, 3, 16} {
		values := topKSmallIndexBenchmarkValues(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				top := newDefaultTopKData()
				for idx, value := range values {
					top.addPlainJSONString(value, uint64(idx+1))
				}
				topKSmallIndexBenchmarkTopSink = top
			}
		})
	}
}

func BenchmarkTopKSmallIndexDuplicate(b *testing.B) {
	for _, size := range []int{1, 2, 3, 16} {
		values := topKSmallIndexBenchmarkValues(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			top := topKSmallIndexBenchmarkTop(values)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				topKSmallIndexBenchmarkEstimateSink = top.addPlainJSONString(values[size-1], 1)
			}
		})
	}
}

func BenchmarkTopKSmallIndexEstimate(b *testing.B) {
	for _, size := range []int{1, 2, 3, 16} {
		values := topKSmallIndexBenchmarkValues(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			top := topKSmallIndexBenchmarkTop(values)
			key := topKPlainJSONStringKey(values[size-1])
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				topKSmallIndexBenchmarkEstimateSink = top.estimateKey(key)
			}
		})
	}
}

func BenchmarkTopKSmallIndexCommand(b *testing.B) {
	for _, size := range []int{1, 2, 3, 16} {
		for _, operation := range []string{"Add", "Estimate", "Get"} {
			b.Run(operation+strconv.Itoa(size), func(b *testing.B) {
				ht := CreateHatTrie()
				defer ht.Destroy()
				values := topKSmallIndexBenchmarkValues(size)
				benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "CREATETOPK", Key: "topk:key", Value: "16"})
				for idx, value := range values {
					benchmarkExecuteCommand(b, ht, CacheCommandRequest{Command: "ADDTOPK", Key: "topk:key", Value: value, Subkey: strconv.Itoa(idx + 1)})
				}
				request := CacheCommandRequest{Command: "GETTOPK", Key: "topk:key"}
				switch operation {
				case "Add":
					request = CacheCommandRequest{Command: "ADDTOPK", Key: "topk:key", Value: values[size-1], Subkey: "1"}
				case "Estimate":
					request = CacheCommandRequest{Command: "ESTTOPK", Key: "topk:key", Value: values[size-1]}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					benchmarkExecuteCommand(b, ht, request)
				}
			})
		}
	}
}

func topKSmallIndexBenchmarkValues(size int) []string {
	values := make([]string, size)
	for idx := range values {
		values[idx] = "value-" + strconv.Itoa(idx)
	}
	return values
}

func topKSmallIndexBenchmarkTop(values []string) topKData {
	top := newDefaultTopKData()
	for idx, value := range values {
		top.addPlainJSONString(value, uint64(idx+1))
	}
	return top
}
