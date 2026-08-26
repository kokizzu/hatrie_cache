package hatCache

import (
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func TestBloomFilterHeaderLayoutIsCompact(t *testing.T) {
	if got := unsafe.Sizeof(bloomFilterData{}); got != 40 {
		t.Fatalf("bloomFilterData size = %d, want 40 bytes", got)
	}
}

func BenchmarkBloomFilterHeaderLayout100k(b *testing.B) {
	const filterCount = 100000

	var retainedBytes, retainedObjects uint64
	var elapsed time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		store := CreateBloomFilterStorage()
		store.array = make([]bloomFilterData, 0, filterCount)
		b.StartTimer()
		started := time.Now()
		for range filterCount {
			store.AddData(newBloomFilterDataWithShape(64, 1))
		}
		elapsed += time.Since(started)
		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			retainedBytes += after.HeapAlloc - before.HeapAlloc
		}
		if after.HeapObjects > before.HeapObjects {
			retainedObjects += after.HeapObjects - before.HeapObjects
		}
		if got := len(store.array); got != filterCount {
			b.Fatalf("stored filters = %d, want %d", got, filterCount)
		}
		runtime.KeepAlive(store)
		b.StartTimer()
	}
	b.StopTimer()
	operations := float64(b.N * filterCount)
	b.ReportMetric(float64(elapsed.Nanoseconds())/operations, "ns/filter")
	b.ReportMetric(float64(retainedBytes)/operations, "retained_B/filter")
	b.ReportMetric(float64(retainedObjects)/operations, "retained_objects/filter")
	b.ReportMetric(float64(unsafe.Sizeof(bloomFilterData{})), "struct_B/filter")
}
