package hatMetrics

import "runtime"

// HeapFragmentationReport is a read-only snapshot of Go heap placement and
// runtime allocator metadata. ReusableBytes is idle heap memory retained by
// the runtime instead of returned to the operating system.
type HeapFragmentationReport struct {
	HeapSysBytes      uint64
	HeapInuseBytes    uint64
	HeapIdleBytes     uint64
	HeapReleasedBytes uint64
	ReusableBytes     uint64
	MSpanInuseBytes   uint64
	MCacheInuseBytes  uint64
	BuckHashSysBytes  uint64
	GCSysBytes        uint64
	OtherSysBytes     uint64
	MetadataBytes     uint64
	StackInuseBytes   uint64
	StackSysBytes     uint64
}

// ReadHeapFragmentationReport reads the current process heap and runtime
// allocator counters. It does not retain state or start background work.
func ReadHeapFragmentationReport() HeapFragmentationReport {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return heapFragmentationReportFromMemStats(stats)
}

func heapFragmentationReportFromMemStats(stats runtime.MemStats) HeapFragmentationReport {
	reusable := uint64(0)
	if stats.HeapIdle > stats.HeapReleased {
		reusable = stats.HeapIdle - stats.HeapReleased
	}
	metadata := saturatingUint64Add(stats.MSpanInuse, stats.MCacheInuse)
	metadata = saturatingUint64Add(metadata, stats.BuckHashSys)
	metadata = saturatingUint64Add(metadata, stats.GCSys)
	metadata = saturatingUint64Add(metadata, stats.OtherSys)
	return HeapFragmentationReport{
		HeapSysBytes:      stats.HeapSys,
		HeapInuseBytes:    stats.HeapInuse,
		HeapIdleBytes:     stats.HeapIdle,
		HeapReleasedBytes: stats.HeapReleased,
		ReusableBytes:     reusable,
		MSpanInuseBytes:   stats.MSpanInuse,
		MCacheInuseBytes:  stats.MCacheInuse,
		BuckHashSysBytes:  stats.BuckHashSys,
		GCSysBytes:        stats.GCSys,
		OtherSysBytes:     stats.OtherSys,
		MetadataBytes:     metadata,
		StackInuseBytes:   stats.StackInuse,
		StackSysBytes:     stats.StackSys,
	}
}

func saturatingUint64Add(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
