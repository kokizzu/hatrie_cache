# Remote-Part Prefetch

`hatStorage.RemotePartCache.Prefetch` adds explicit, bounded read-ahead for
immutable remote parts. It reuses the cache's checksum/size validation,
single-flight loading, priority-aware admission, and byte/entry limits.

```go
err := cache.Prefetch(ctx, references, hatStorage.RemotePartPrefetchOptions{
	MaxConcurrent: 2,
	Priority:      1,
}, loader)
```

`MaxConcurrent` defaults to `hatStorage.DefaultRemotePartPrefetchConcurrency`
(2). Duplicate references are loaded once. A failed load cancels remaining
prefetch work and returns the first error. The caller's context is never
cancelled by an internal prefetch failure. `Get` remains the read-through
fallback for a part that was not prefetched or admitted to the bounded cache.

Prefetch is not automatic: callers opt in per read sequence. This keeps
bandwidth, transient loader memory, and cache pollution under caller control.
Use `Acquire` when a concurrent reader must pin a part during use.

## Measurement

The benchmark target is:

```text
make benchmark-remote-part-prefetch-after
```

It uses 16 unique 4 KiB parts and compares sequential `Get` calls with
`Prefetch(MaxConcurrent: 2)`. The remote case adds a 100 microsecond sleep to
each loader call to model I/O latency; it is directional, not an object-store
SLA.

| Workload | Sequential median | Prefetch median | Result | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zero-latency loader | 46,513 ns | 69,509 ns | 1.49x slower | 139,017 -> 142,415 | 87 -> 100 |
| 100 us loader latency | 16,892,372 ns | 8,507,850 ns | 1.99x faster | 139,108 -> 142,585 | 90 -> 103 |

The default cache read path is unchanged. Use prefetch only when remote wait
dominates worker and allocation overhead.
