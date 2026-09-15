# TR-052 Small-Vector OrderedIndex Representation

`OrderedIndex` is already a sorted vector, but its old implementation retained
a `map[uint64]int` for every non-empty index. That map is useful for larger
indexes, while its hashing and bucket footprint are unnecessary for the small
secondary indexes commonly created by SQL and metadata paths.

The implementation now keeps ID positions in the sorted vector while the index
contains at most 32 entries. ID lookup is a linear scan in that state. Inserting
entry 33 promotes the index to the existing position-map path. Deleting back to
32 entries releases the map again. The promotion uses the vector capacity as a
map capacity hint, so a pre-sized larger index does not repeatedly grow its map.

The threshold is an internal constant. It does not change ordering, duplicate-ID
replacement, iterator invalidation, snapshot behavior, or concurrency semantics.
Both ordinary mutations and copy-on-write mutations used by live iterators are
covered by regression tests.

## Measurement

Linux/amd64 on an AMD Ryzen 9 5950X. The baseline is the map-backed
implementation before the change; the after values are the same benchmark
after the change. Every measured mutation path reported `0 B/op` and
`0 allocs/op` after setup.

| Entries | Before | After | Improvement |
| ---: | ---: | ---: | ---: |
| 1 | 47.01 ns/op | 16.09 ns/op | 2.92x faster |
| 4 | 76.52 ns/op | 25.78 ns/op | 2.97x faster |
| 8 | 134.5 ns/op | 26.94 ns/op | 4.99x faster |
| 16 | 254.0 ns/op | 35.09 ns/op | 7.24x faster |
| 32 | 411.0 ns/op | 41.92 ns/op | 9.80x faster |
| 64 | 778.7 ns/op | 754.9 ns/op | 1.03x faster; within normal run variance |

The direct lookup microbenchmark showed the same crossover: linear lookup was
faster at 32 entries (`5.79 ns/op` versus `6.44 ns/op` for a map), while a map
was much faster at 64 entries (`6.51 ns/op` versus `16.26 ns/op`). This is why
the default remains automatic promotion rather than a permanently linear
representation.

For entries at or below the threshold, the position map is nil and its buckets
are not retained. Above the threshold, the existing map-backed behavior remains
in effect. The only added cost is the one-time promotion allocation when an
index crosses 32 entries; it is outside the steady-state mutation benchmark.

Command: `make benchmark-ordered-index-c203`

Correctness commands:

- `make verify-ordered-index-c203`
- `make test-ordered-index-c203` (the package-wide run currently reaches the
  unrelated existing `TestSpillableArrangementSpillsReadsDeletesAndCompacts`
  failure before the focused race and vet checks)
