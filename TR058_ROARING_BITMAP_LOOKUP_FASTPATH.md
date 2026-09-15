# Roaring Bitmap Lookup Fast Path

Roaring bitmap container lookup now uses explicit lower-bound loops for the
sorted high-word container list and sparse array-container values. This
removes callback-based `sort.Search` overhead from `Contains`, `Add`, and
`Remove` while preserving the bitmap-container bit test and sorted ordering.

The change is internal, automatic, and has no wire or storage format impact.
Bitmap containers continue to use their existing fixed bitset path. The
boundary test covers the first and last containers, present values, missing
values, and values beyond the bitmap.

## Measurement

The benchmark was run on Linux/amd64 with an AMD Ryzen 9 5950X. Each result
below is the median of three samples from
`make benchmark-roaring-lookup-fastpath-c211`; all cases reported zero
allocations before and after.

| Workload | Before | After | Relative result | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| Sparse container hit | 27.78 ns/op | 25.41 ns/op | 1.09x faster | 0 | 0 | 0 | 0 |
| Sparse container miss | 28.18 ns/op | 23.52 ns/op | 1.20x faster | 0 | 0 | 0 | 0 |
| Array container hit | 12.94 ns/op | 11.51 ns/op | 1.12x faster | 0 | 0 | 0 | 0 |
| Array container miss | 12.61 ns/op | 10.99 ns/op | 1.15x faster | 0 | 0 | 0 | 0 |

Raw baseline samples:

```text
sparse_container_hit: 32.95 27.78 26.45 ns/op; 0 B/op; 0 allocs/op
sparse_container_miss: 28.18 29.15 27.14 ns/op; 0 B/op; 0 allocs/op
array_container_hit: 12.11 13.34 12.94 ns/op; 0 B/op; 0 allocs/op
array_container_miss: 12.35 12.90 12.61 ns/op; 0 B/op; 0 allocs/op
```

Raw final samples:

```text
sparse_container_hit: 25.77 24.50 25.41 ns/op; 0 B/op; 0 allocs/op
sparse_container_miss: 23.22 23.59 23.52 ns/op; 0 B/op; 0 allocs/op
array_container_hit: 12.15 11.37 11.51 ns/op; 0 B/op; 0 allocs/op
array_container_miss: 11.16 10.99 10.95 ns/op; 0 B/op; 0 allocs/op
```

Commands:

```text
make test-roaring-lookup-fastpath-c211
make benchmark-roaring-lookup-fastpath-c211
make verify-roaring-lookup-fastpath-c211
```
