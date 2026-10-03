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

The threshold is deliberately 16. A 32-row vector improved construction but
made 32-row lookups about 20% slower than the map control. The lower threshold
keeps the lookup crossover neutral while retaining the large memory win for
the genuinely small case.

## Measurements

Five samples with `-benchtime=100ms -count=5` on Linux/amd64, AMD Ryzen 9
5950X. The before run used the map-only implementation; the after run used
the adaptive representation. `x` compares before/after, so higher is better
for time, while lower B/op and allocs/op are better for memory.

| Workload | Before median | After median | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small upsert, 4 rows | 23.15 ns/op | 11.17 ns/op | 2.07x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small upsert, 16 rows | 24.15 ns/op | 12.70 ns/op | 1.90x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small lookup, 4 rows | 13.33 ns/op | 7.60 ns/op | 1.75x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small lookup, 16 rows | 18.79 ns/op | 14.97 ns/op | 1.26x faster | 0 B/op | 0 B/op | 0 | 0 |
| Small build, 4 rows | 3,755 ns/op | 145.6 ns/op | 25.8x faster | 27,360 B/op | 464 B/op | 9 | 2 |
| Small build, 16 rows | 4,465 ns/op | 324.6 ns/op | 13.8x faster | 27,584 B/op | 464 B/op | 21 | 2 |
| 32-row lookup control | 19.86 ns/op | 20.07 ns/op | 0.99x, neutral | 0 B/op | 0 B/op | 0 | 0 |
| 256-row build control | 20,747 ns/op | 21,385 ns/op | 0.97x, 3.1% slower | 32,224 B/op | 33,408 B/op | 169 | 171 |

The large-control cost is the one-time small-vector allocation and promotion
copy. It is bounded and disappears from retained memory after promotion, but
it remains in cumulative allocation accounting. The feature is accepted
because the target small-index workloads remove roughly 27 KB of map backing
and 7 to 19 allocations per build; the 16-row threshold avoids making the
lookup path slower.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#tr-054-functional-index-small-vector).
