package hatMonitoring

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"time"
)

// MemoryReport is an on-demand snapshot of Go allocator and garbage collector
// state. It is intentionally separate from the cache's per-key statistics so
// collecting it has no cost on normal cache operations.
type MemoryReport struct {
	CollectedAt time.Time `json:"collected_at"`

	AllocBytes      uint64 `json:"alloc_bytes"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
	Lookups         uint64 `json:"lookups"`
	Mallocs         uint64 `json:"mallocs"`
	Frees           uint64 `json:"frees"`

	HeapAllocBytes    uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes      uint64 `json:"heap_sys_bytes"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapInuseBytes    uint64 `json:"heap_inuse_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapObjects       uint64 `json:"heap_objects"`

	StackInuseBytes  uint64 `json:"stack_inuse_bytes"`
	StackSysBytes    uint64 `json:"stack_sys_bytes"`
	MSpanInuseBytes  uint64 `json:"mspan_inuse_bytes"`
	MSpanSysBytes    uint64 `json:"mspan_sys_bytes"`
	MCacheInuseBytes uint64 `json:"mcache_inuse_bytes"`
	MCacheSysBytes   uint64 `json:"mcache_sys_bytes"`
	BuckHashSysBytes uint64 `json:"buck_hash_sys_bytes"`
	GCSysBytes       uint64 `json:"gc_sys_bytes"`
	OtherSysBytes    uint64 `json:"other_sys_bytes"`

	NextGCBytes     uint64  `json:"next_gc_bytes"`
	LastGCUnixNano  int64   `json:"last_gc_unix_nano"`
	NumGC           uint32  `json:"num_gc"`
	NumForcedGC     uint32  `json:"num_forced_gc"`
	PauseTotalNs    uint64  `json:"pause_total_ns"`
	GCCPUFraction   float64 `json:"gc_cpu_fraction"`
	GOMemLimitBytes int64   `json:"go_mem_limit_bytes"`
	GOGCPercent     int64   `json:"gogc_percent"`

	HeapObjectsClassBytes  uint64 `json:"heap_objects_class_bytes"`
	HeapFreeClassBytes     uint64 `json:"heap_free_class_bytes"`
	HeapReleasedClassBytes uint64 `json:"heap_released_class_bytes"`
	HeapUnusedClassBytes   uint64 `json:"heap_unused_class_bytes"`
	MetadataClassBytes     uint64 `json:"metadata_class_bytes"`
	OSTacksClassBytes      uint64 `json:"os_stacks_class_bytes"`
	OtherClassBytes        uint64 `json:"other_class_bytes"`
	TotalClassBytes        uint64 `json:"total_class_bytes"`
}

const (
	memoryMetricHeapObjects = iota
	memoryMetricHeapFree
	memoryMetricHeapReleased
	memoryMetricHeapUnused
	memoryMetricMetadata
	memoryMetricOSTacks
	memoryMetricOther
	memoryMetricTotal
	memoryMetricGOMemLimit
	memoryMetricGOGC
)

var memoryMetricNames = [...]string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/free:bytes",
	"/memory/classes/heap/released:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/metadata/total:bytes",
	"/memory/classes/os-stacks:bytes",
	"/memory/classes/other:bytes",
	"/memory/classes/total:bytes",
	"/gc/gomemlimit:bytes",
	"/gc/gogc:percent",
}

var memoryMetricSamplesPool = sync.Pool{
	New: func() interface{} {
		return new([len(memoryMetricNames)]metrics.Sample)
	},
}

// ReadMemoryReport reads a bounded snapshot of process memory state. It does
// not trigger GC, change runtime settings, or retain any process data.
func ReadMemoryReport() MemoryReport {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	samples := memoryMetricSamplesPool.Get().(*[len(memoryMetricNames)]metrics.Sample)
	defer memoryMetricSamplesPool.Put(samples)
	for index, name := range memoryMetricNames {
		samples[index].Name = name
	}
	metrics.Read(samples[:])

	return MemoryReport{
		CollectedAt:            time.Now().UTC(),
		AllocBytes:             stats.Alloc,
		TotalAllocBytes:        stats.TotalAlloc,
		SysBytes:               stats.Sys,
		Lookups:                stats.Lookups,
		Mallocs:                stats.Mallocs,
		Frees:                  stats.Frees,
		HeapAllocBytes:         stats.HeapAlloc,
		HeapSysBytes:           stats.HeapSys,
		HeapIdleBytes:          stats.HeapIdle,
		HeapInuseBytes:         stats.HeapInuse,
		HeapReleasedBytes:      stats.HeapReleased,
		HeapObjects:            stats.HeapObjects,
		StackInuseBytes:        stats.StackInuse,
		StackSysBytes:          stats.StackSys,
		MSpanInuseBytes:        stats.MSpanInuse,
		MSpanSysBytes:          stats.MSpanSys,
		MCacheInuseBytes:       stats.MCacheInuse,
		MCacheSysBytes:         stats.MCacheSys,
		BuckHashSysBytes:       stats.BuckHashSys,
		GCSysBytes:             stats.GCSys,
		OtherSysBytes:          stats.OtherSys,
		NextGCBytes:            stats.NextGC,
		LastGCUnixNano:         int64(stats.LastGC),
		NumGC:                  stats.NumGC,
		NumForcedGC:            stats.NumForcedGC,
		PauseTotalNs:           stats.PauseTotalNs,
		GCCPUFraction:          stats.GCCPUFraction,
		HeapObjectsClassBytes:  memoryMetricUint64(samples[memoryMetricHeapObjects]),
		HeapFreeClassBytes:     memoryMetricUint64(samples[memoryMetricHeapFree]),
		HeapReleasedClassBytes: memoryMetricUint64(samples[memoryMetricHeapReleased]),
		HeapUnusedClassBytes:   memoryMetricUint64(samples[memoryMetricHeapUnused]),
		MetadataClassBytes:     memoryMetricUint64(samples[memoryMetricMetadata]),
		OSTacksClassBytes:      memoryMetricUint64(samples[memoryMetricOSTacks]),
		OtherClassBytes:        memoryMetricUint64(samples[memoryMetricOther]),
		TotalClassBytes:        memoryMetricUint64(samples[memoryMetricTotal]),
		GOMemLimitBytes:        memoryMetricInt64(samples[memoryMetricGOMemLimit]),
		GOGCPercent:            memoryMetricInt64(samples[memoryMetricGOGC]),
	}
}

func memoryMetricUint64(sample metrics.Sample) uint64 {
	if sample.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample.Value.Uint64()
}

func memoryMetricInt64(sample metrics.Sample) int64 {
	if sample.Value.Kind() == metrics.KindUint64 {
		value := sample.Value.Uint64()
		if value <= uint64(^uint64(0)>>1) {
			return int64(value)
		}
	}
	return 0
}
