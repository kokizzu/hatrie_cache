# SparseBitset `ValuesInto`

`SparseBitset.ValuesInto` enumerates set values in sorted order while reusing a
caller-owned `[]uint64` buffer. It is intended for repeated scans where the
caller can retain one result buffer between calls.

```go
values := make([]uint64, 0, bitset.Count())
for range requests {
    values = bitset.ValuesInto(values)
    consume(values)
}
```

The method clears and reuses the supplied slice, grows it only when its
capacity is insufficient, and never exposes the bitset's internal container
storage. The existing `Values()` method remains unchanged for callers that
prefer a newly returned slice.

## Measurement

Workload: a sparse bitset containing 100,000 sequential values, Go benchmark
with `-benchmem -count=10`.

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `Values()` | 216,916 | 802,816 | 1 |
| `ValuesInto()` with reused buffer | 127,518 | 0 | 0 |

The reusable path was about **1.70x faster** and removed the per-call result
allocation. The first call still pays for a buffer when the supplied capacity
is too small; subsequent calls should pass the same returned slice.
