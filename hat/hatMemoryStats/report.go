// Package hatMemoryStats exposes small, read-only memory diagnostics.
package hatMemoryStats

import (
	"runtime/metrics"
)

const (
	heapObjectsMetric  = "/memory/classes/heap/objects:bytes"
	heapFreeMetric     = "/memory/classes/heap/free:bytes"
	heapReleasedMetric = "/memory/classes/heap/released:bytes"
	heapStacksMetric   = "/memory/classes/heap/stacks:bytes"
	totalMetric        = "/memory/classes/total:bytes"
)

// Values contains the runtime byte counters used by Compute.
//
// Callers can construct Values directly when they already have allocator
// counters, or use Snapshot to read the counters exposed by Go.
type Values struct {
	HeapObjectsBytes  uint64
	HeapFreeBytes     uint64
	HeapReleasedBytes uint64
	HeapStacksBytes   uint64
	TotalBytes        uint64
}

// Report is a point-in-time summary of runtime memory counters.
type Report struct {
	Values

	HeapReservedBytes  uint64
	ReclaimableBytes   uint64
	FragmentationRatio float64
	ReclaimableRatio   float64
	KnownMetrics       uint8
}

// Compute derives bounded ratios and reclaimable bytes from runtime values.
// Missing or inconsistent counters produce zero for the affected metric.
func Compute(values Values) Report {
	report := Report{Values: values}
	report.HeapReservedBytes = saturatingAdd(values.HeapObjectsBytes, values.HeapFreeBytes)
	if values.HeapFreeBytes > values.HeapReleasedBytes {
		report.ReclaimableBytes = values.HeapFreeBytes - values.HeapReleasedBytes
	}
	if report.HeapReservedBytes != 0 {
		report.FragmentationRatio = float64(values.HeapFreeBytes) / float64(report.HeapReservedBytes)
	}
	if values.TotalBytes != 0 {
		report.ReclaimableRatio = float64(report.ReclaimableBytes) / float64(values.TotalBytes)
		if report.ReclaimableRatio > 1 {
			report.ReclaimableRatio = 1
		}
	}
	return report
}

// Snapshot reads the Go runtime memory metrics and computes a Report.
// Metrics unavailable in the current Go runtime are left at zero.
func Snapshot() Report {
	samples := [...]metrics.Sample{
		{Name: heapObjectsMetric},
		{Name: heapFreeMetric},
		{Name: heapReleasedMetric},
		{Name: heapStacksMetric},
		{Name: totalMetric},
	}
	metrics.Read(samples[:])

	values := Values{}
	var known uint8
	if value, ok := sampleUint64(samples[0]); ok {
		values.HeapObjectsBytes = value
		known++
	}
	if value, ok := sampleUint64(samples[1]); ok {
		values.HeapFreeBytes = value
		known++
	}
	if value, ok := sampleUint64(samples[2]); ok {
		values.HeapReleasedBytes = value
		known++
	}
	if value, ok := sampleUint64(samples[3]); ok {
		values.HeapStacksBytes = value
		known++
	}
	if value, ok := sampleUint64(samples[4]); ok {
		values.TotalBytes = value
		known++
	}

	report := Compute(values)
	report.KnownMetrics = known
	return report
}

func sampleUint64(sample metrics.Sample) (uint64, bool) {
	if sample.Value.Kind() != metrics.KindUint64 {
		return 0, false
	}
	return sample.Value.Uint64(), true
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
