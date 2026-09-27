# M039: Incremental Group Count Distinct

`IncrementalGroupCountDistinctInt64` maintains exact differential
`COUNT(DISTINCT int64)` values grouped by a caller-provided key. It retains a
multiplicity for every value in every live group, so duplicate inserts and
retractions do not change the visible distinct count until a value enters or
leaves the group.

## Behavior

- `NewIncrementalGroupCountDistinctInt64` requires group-key and value
  callbacks.
- `Apply` accepts positive inserts and negative retractions.
- Duplicate value multiplicity changes with no distinct-membership change emit
  no aggregate rows.
- A changed distinct count emits an old-value retraction followed by a new-value
  insertion; groups entering or leaving emit one row.
- Callback errors, negative group counts, negative value multiplicities, and
  checked overflow reject the complete batch without changing retained state.
- `Snapshot` returns one aggregate row per live group in lexical key order.
- The operator is single-writer and requires caller synchronization when shared
  between goroutines.

## Benchmark

Command:

```text
make benchmark-m039-incremental-group-count-distinct
```

Workload: 10,000 retained rows in 100 groups, with 100 distinct values per
group. Each timed iteration turns over two values in `group-42`: retract one
present value and insert one absent value. The pair alternates each iteration,
keeping retained state bounded and the visible distinct count at 100. Rebuild
re-aggregates all 10,002 rows for every iteration; the incremental operator
applies only the two-row batch. Five samples ran on an AMD Ryzen 9 5950X.

Raw result:

```text
BenchmarkM039RebuildGroupCountDistinct-32          211  5829396 ns/op  7965739 B/op  40919 allocs/op
BenchmarkM039RebuildGroupCountDistinct-32          210  5738257 ns/op  7965712 B/op  40919 allocs/op
BenchmarkM039RebuildGroupCountDistinct-32          187  5797933 ns/op  7965710 B/op  40919 allocs/op
BenchmarkM039RebuildGroupCountDistinct-32          212  5181275 ns/op  7965704 B/op  40919 allocs/op
BenchmarkM039RebuildGroupCountDistinct-32          224  5445540 ns/op  7965707 B/op  40919 allocs/op
BenchmarkM039IncrementalGroupCountDistinct-32  329905       3616 ns/op     2472 B/op      5 allocs/op
BenchmarkM039IncrementalGroupCountDistinct-32  326438       3698 ns/op     2472 B/op      5 allocs/op
BenchmarkM039IncrementalGroupCountDistinct-32  325747       3644 ns/op     2472 B/op      5 allocs/op
BenchmarkM039IncrementalGroupCountDistinct-32  341678       3660 ns/op     2472 B/op      5 allocs/op
BenchmarkM039IncrementalGroupCountDistinct-32  325999       3654 ns/op     2472 B/op      5 allocs/op
```

Median comparison:

| Path | Time | Transient bytes | Allocs | Relative to rebuild |
| --- | ---: | ---: | ---: | ---: |
| Rebuild | 5,738,257 ns/op | 7,965,712 B/op | 40,919 | 1.00x |
| Incremental two-value turnover | 3,654 ns/op | 2,472 B/op | 5 | 1,571x faster, 3,222x lower transient bytes, 8,184x fewer allocs |

The `B/op` column measures transient update allocations, not retained state.
The incremental operator intentionally retains one map entry per live group and
per distinct value, so its steady-state memory grows with cardinality. That is
the tradeoff for avoiding a full scan; workloads with very high cardinality
should measure retained heap and use a bounded lifecycle or snapshot policy.

## Inspiration

This is a Materialize-style differential arrangement for grouped distinct
membership, using the same per-value multiplicity principle as the existing
batch helper `GroupCountDistinctInt64DifferentialRows`. It also follows the
ClickHouse pattern of retaining distinct-state sketches/sets close to the
aggregation key, but keeps this exact implementation opt-in and int64-specific.
