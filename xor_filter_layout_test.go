package hatriecache

import (
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

type legacyXorFilterDataLayout struct {
	expectedItems uint64
	built         bool
	items         uint64
	seed          uint64
	blockLength   uint32
	fingerprints  []uint8
	staged        map[string]interface{}
}

func (filter legacyXorFilterDataLayout) containsJSONString(value string) (bool, bool) {
	if !filter.built {
		return false, false
	}
	if filter.blockLength == 0 || len(filter.fingerprints) == 0 {
		return false, true
	}
	if len(filter.fingerprints) != int(filter.blockLength)*3 {
		return false, false
	}
	hash := xorFilterHashJSONString(value, filter.seed)
	fingerprint := xorFilterFingerprint(hash)
	for _, index := range xorFilterIndexes(hash, filter.blockLength) {
		fingerprint ^= filter.fingerprints[index]
	}
	return fingerprint == 0, true
}

var benchmarkXorFilterLayoutLookupSink bool

func TestXorFilterHeaderLayoutIsBounded(t *testing.T) {
	if got := unsafe.Sizeof(xorFilterData{}); got != 64 {
		t.Fatalf("xorFilterData size = %d, want 64 bytes", got)
	}
	if got := unsafe.Sizeof(legacyXorFilterDataLayout{}); got != 72 {
		t.Fatalf("legacy xorFilterData control size = %d, want 72 bytes", got)
	}
}

func BenchmarkXorFilterHeaderLayout100k(b *testing.B) {
	const filterCount = 100000

	var (
		retainedBytes   uint64
		retainedObjects uint64
		elapsed         time.Duration
	)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		store := CreateXorFilterStorage()
		store.array = make([]xorFilterData, 0, filterCount)
		b.StartTimer()
		started := time.Now()
		for range filterCount {
			store.AddData(newDefaultXorFilterData())
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
	b.ReportMetric(float64(unsafe.Sizeof(xorFilterData{})), "struct_B/filter")
}

func BenchmarkXorFilterHeaderLookupLayout(b *testing.B) {
	filter, err := newXorFilterData(64)
	if err != nil {
		b.Fatal(err)
	}
	for item := range 64 {
		if _, err := filter.addJSONString(fmt.Sprintf("value-%d", item)); err != nil {
			b.Fatal(err)
		}
	}
	if err := filter.Build(); err != nil {
		b.Fatal(err)
	}
	legacy := legacyXorFilterDataLayout{
		expectedItems: filter.expectedItems,
		built:         filter.built,
		items:         filter.items,
		seed:          filter.seed,
		blockLength:   filter.blockLength,
		fingerprints:  filter.fingerprints,
		staged:        filter.staged,
	}

	b.Run("Legacy72", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			hit, queryable := legacy.containsJSONString("value-31")
			benchmarkXorFilterLayoutLookupSink = hit && queryable
		}
	})
	b.Run("Compact64", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			hit, queryable := filter.containsJSONString("value-31")
			benchmarkXorFilterLayoutLookupSink = hit && queryable
		}
	})
}
