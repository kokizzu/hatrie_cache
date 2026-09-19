# CH-U18 Composite Primary-Mark Pruning

`hat/hatPrimaryPruning` provides immutable packed composite mark bounds for
ordered numeric tuples. It compares lexicographic lower/upper bounds, supports
explicit `NULLS FIRST` or `NULLS LAST`, and returns candidate mark indexes into
a caller-reused slice.

```go
index, _ := hatPrimaryPruning.Build(marks)
candidates, _ := index.Candidates(queryRange, scratch[:0])
```

Marks are copied into one contiguous bound buffer. If the marks are monotone
and non-overlapping, pruning binary-searches the first and last possible mark
before checking the small interval. Unordered or overlapping marks safely fall
back to a full scan, preserving correctness. The implementation only reports
possible matches; the caller still verifies rows after reading selected marks.

Five-run medians on the local Ryzen 9 5950X with 10,000 two-column marks:

| Operation | CPU | Memory | Allocations |
|---|---:|---:|---:|
| Selective linear scan | 59.81 us/op | 0 B/op | 0 |
| Selective packed pruning | 98.15 ns/op | 0 B/op | 0 |
| Full-range linear scan | 47.09 us/op | 0 B/op | 0 |
| Full-range packed pruning | 36.95 us/op | 0 B/op | 0 |
| One-mark `MayOverlap` | 17.14 ns/op | 0 B/op | 0 |
| Build packed marks | 233.77 us/op | 647 KB/op | 2 |

The selective path is about 609x faster in this workload and the full-range
path is about 1.27x faster. Build once and reuse the immutable index; callers
should supply a reusable candidate buffer to keep query pruning allocation-free.
