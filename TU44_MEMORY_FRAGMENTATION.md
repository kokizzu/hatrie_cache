# T-U44 Memory Fragmentation Diagnostics

`hat/hatMemoryStats` provides a read-only, portable memory report for
operational diagnostics. It uses Go `runtime/metrics`, so it does not add
allocator state, background work, or a network endpoint to the data path.

## Usage

```go
report := hatMemoryStats.Snapshot()
fmt.Printf("reserved=%d reclaimable=%d fragmentation=%.3f known=%d\n",
    report.HeapReservedBytes,
    report.ReclaimableBytes,
    report.FragmentationRatio,
    report.KnownMetrics,
)
```

`Compute` is available when an application already collects the counters:

```go
report := hatMemoryStats.Compute(hatMemoryStats.Values{
    HeapObjectsBytes:  objects,
    HeapFreeBytes:     free,
    HeapReleasedBytes: released,
    HeapStacksBytes:   stacks,
    TotalBytes:        total,
})
```

`HeapReservedBytes` is heap objects plus heap free bytes. `ReclaimableBytes`
is free heap that has not yet been released. `FragmentationRatio` is free
heap divided by reserved heap. `ReclaimableRatio` is reclaimable bytes divided
by total runtime bytes. Inconsistent input is clamped rather than producing a
ratio above 1. `KnownMetrics` reports how many of the five requested runtime
metrics were available on the running Go version.

This is an approximation, not per-size-class slab telemetry. It is useful for
detecting retained free heap and changes over time; use allocator-specific
profilers when exact size-class fragmentation is required.

## Cost

Measured on AMD Ryzen 9 5950X, linux/amd64, with
`go test ./hat/hatMemoryStats -run '^$' -bench '^Benchmark(Compute|Snapshot)$' -benchmem -count=5`:

| Operation | Median | Memory | Allocations |
| --- | ---: | ---: | ---: |
| `Compute` | 12.21 ns/op | 0 B/op | 0 allocs/op |
| `Snapshot` | 596.7 ns/op | 208 B/op | 1 allocs/op |

`Compute` is suitable for a hot diagnostic calculation. `Snapshot` should be
sampled at an operational interval rather than called for every request. The
allocation comes from the Go runtime metric read path; callers that do not
need a fresh runtime sample can reuse a `Values` report with zero allocations.

## Safety

The package only reads process-local runtime counters. It has no persistence,
network listener, credentials, or mutation of Hatrie data structures.
