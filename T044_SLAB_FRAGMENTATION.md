# T-U44 Heap Fragmentation Diagnostics

T-U44 adds hatMetrics.ReadHeapFragmentationReport, an opt-in read-only
snapshot of the Go runtime heap and allocator metadata. It is useful for
operator dashboards and capacity investigations without adding a registry,
background sampler, or work to cache operations.

## Example

~~~go
report := hatMetrics.ReadHeapFragmentationReport()
fmt.Printf(
	"heap=%d inuse=%d idle=%d released=%d reusable=%d metadata=%d\\n",
	report.HeapSysBytes,
	report.HeapInuseBytes,
	report.HeapIdleBytes,
	report.HeapReleasedBytes,
	report.ReusableBytes,
	report.MetadataBytes,
)
~~~

## Fields

HeapSysBytes, HeapInuseBytes, HeapIdleBytes, and HeapReleasedBytes are the
corresponding runtime.MemStats counters. ReusableBytes is calculated as
max(HeapIdleBytes - HeapReleasedBytes, 0): memory the runtime can reuse
without requesting more address space but has not returned to the operating
system.

The report also exposes MSpanInuseBytes, MCacheInuseBytes, BuckHashSysBytes,
GCSysBytes, and OtherSysBytes. MetadataBytes is their saturating sum.
StackInuseBytes and StackSysBytes provide the corresponding goroutine-stack
footprint.

The report uses portable Go runtime counters rather than platform-specific
allocator internals. It should therefore be interpreted as runtime heap
fragmentation/reuse telemetry, not as a precise OS slab map.

## Cost

The benchmark compares a direct runtime.ReadMemStats control with the public
report function on an AMD Ryzen 9 5950X, using Go benchmarks, -benchmem, and
-count=5:

| Path | Samples (ns/op) | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct runtime.ReadMemStats control | 19,173; 20,708; 20,495; 19,614; 18,454 | 19,614 | 0 | 0 |
| ReadHeapFragmentationReport | 19,268; 19,402; 18,885; 18,991; 19,116 | 19,116 | 0 | 0 |

The medians overlap normal runtime sampling noise; the small timing difference
is not claimed as a speed improvement. The report adds only value copies and
saturating arithmetic, with no measured heap allocation. Because
runtime.ReadMemStats is a diagnostic operation, callers should sample it
outside latency-sensitive request paths.

## Verification

~~~text
make test-t044
make verify-t044
make benchmark-t044
~~~
