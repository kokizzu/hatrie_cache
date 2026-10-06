# TR-054 Functional Index Small Vector

`FunctionalIndex` now uses a bounded small vector while it contains at most
16 rows. It promotes to the existing map plus compact posting-list
representation on the 17th row. `ConditionalFunctionalIndex` keeps the same
predicate and membership semantics through the optimized path.

This is a Tarantool-inspired compact-space optimization: tiny secondary
indexes are common in embedded workloads, and map buckets are expensive when
the index has only a few rows. The public constructors and methods are
unchanged. Posting order remains insertion order; replacing a row under a new
derived key removes it from the old posting and appends it to the new one.
`Clear` returns the index to the empty small representation.

The threshold is deliberately 16. In the measured run, the 32-row promoted
path was 2.8% slower for lookup and the 256-row build control was 1.9% slower
than map-only construction. The lower threshold bounds that promotion cost
while retaining the large memory win for genuinely small indexes.

## Measurements

Five samples with `-benchmem -count=5` on Linux/amd64, AMD Ryzen 9 5950X. The
before run used the map-only implementation; the after run used the adaptive
representation. `x` compares before/after, so higher is better for time,
while lower B/op and allocs/op are better for memory.

| Workload | Before median | After median | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small upsert, 4 rows | 23.62 ns/op | 11.38 ns/op | 2.08x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small upsert, 16 rows | 23.83 ns/op | 14.08 ns/op | 1.69x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small lookup, 4 rows | 13.87 ns/op | 8.035 ns/op | 1.73x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small lookup, 16 rows | 19.06 ns/op | 17.74 ns/op | 1.07x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small build, 4 rows | 3,894 ns/op | 155.0 ns/op | 25.1x faster | 27,360 B/op | 464 B/op | 9 | 2 |
| Small build, 16 rows | 5,010 ns/op | 348.3 ns/op | 14.4x faster | 27,584 B/op | 464 B/op | 21 | 2 |
| 32-row lookup control | 20.15 ns/op | 20.72 ns/op | 0.97x, 2.8% slower | 0 B/op | 0 B/op | 0 | 0 |
| 256-row build control | 21,106 ns/op | 21,506 ns/op | 0.98x, 1.9% slower | 32,224 B/op | 33,408 B/op | 169 | 171 |

The large-control cost is the one-time small-vector allocation and promotion
copy. It is bounded and disappears from retained memory after promotion, but
it remains in cumulative allocation accounting. The feature is accepted
because the target small-index workloads remove roughly 27 KB of map backing
and 7 to 19 allocations per build; the 16-row threshold avoids making the
lookup path slower.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#tr-054-functional-index-small-vector).
