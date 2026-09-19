# CH-U17 Adaptive Exact `IN` Sets

`hat/hatMembership` provides an immutable exact `uint64` membership set for
large constant predicates. It selects a sorted vector for small inputs, a
dense bitmap for compact ranges, and a hash table for large sparse inputs.
Construction copies and deduplicates the input; `Contains` is safe for
concurrent readers and does not allocate.

```go
set := hatMembership.BuildUint64(values)
if set.Contains(candidate) {
	// exact match
}
```

The default thresholds are 32 values for sorted mode, a bitmap span of 1 MiB
when the range is at most 64 times the unique count, and hash mode from 256
unique sparse values. `BuildUint64WithConfig` exposes those thresholds for a
planner or workload-specific policy. `MemoryBytes` is an estimate for hash
mode because map bucket overhead depends on the Go runtime.

Five-run medians on the local Ryzen 9 5950X:

| Operation | CPU | Memory | Allocations |
|---|---:|---:|---:|
| Linear scan, 32 values | 11.78 ns/op | 0 B/op | 0 |
| Adaptive sorted vector | 5.41 ns/op | 0 B/op | 0 |
| Adaptive dense bitmap | 2.23 ns/op | 0 B/op | 0 |
| Adaptive sparse hash | 8.10 ns/op | 0 B/op | 0 |
| Build dense bitmap, 1,000 values | 6.70 us/op | 8.6 KB/op | 5 |
| Build sparse hash, 512 values | 13.31 us/op | 22.8 KB/op | 8 |

The set is intended for build-once/query-many use. Rebuilding for every row is
an anti-pattern; the caller owns caching and planner lifetime decisions.
