# M038: Incremental Group Count and Sum

`IncrementalGroupCountSumInt64` is a retained differential `GROUP BY` operator
for exact `COUNT(*)` and signed `SUM(int64)` values. It keeps one compact
state pair per live group and emits only changed aggregate rows.

## Behavior

- `NewIncrementalGroupCountSumInt64` requires group-key and value callbacks.
- `Apply` accepts positive inserts and negative retractions.
- A changed group emits an old aggregate retraction followed by a new
  aggregate insertion.
- Empty and newly created groups use the same transition rules as the batch
  differential helpers.
- Callback errors, negative counts, multiplication overflow, and sum overflow
  reject the whole batch without mutating retained state.
- `Snapshot` returns positive aggregate rows in lexical group-key order.
- The operator is single-writer and requires caller synchronization when shared
  between goroutines.

## Benchmark

Command:

```text
make benchmark-m038-incremental-group-count-sum
```

Workload: 10,000 retained rows in 100 groups, followed by one new row in
`group-42`. Rebuild re-aggregates all 10,001 rows on every iteration. The
incremental benchmark initializes retained state outside the timed region and
times one new row update. Five samples ran on an AMD Ryzen 9 5950X.

Raw result:

```text
BenchmarkM038RebuildGroupCountSum-32          241  4688184 ns/op  8260220 B/op  54123 allocs/op
BenchmarkM038RebuildGroupCountSum-32          252  4796826 ns/op  8260217 B/op  54123 allocs/op
BenchmarkM038RebuildGroupCountSum-32          249  4565414 ns/op  8260196 B/op  54122 allocs/op
BenchmarkM038RebuildGroupCountSum-32          252  4920763 ns/op  8260200 B/op  54123 allocs/op
BenchmarkM038RebuildGroupCountSum-32          246  4800928 ns/op  8260195 B/op  54123 allocs/op
BenchmarkM038IncrementalGroupCountSum-32  2355843      513.4 ns/op      784 B/op      8 allocs/op
BenchmarkM038IncrementalGroupCountSum-32  2284155      518.7 ns/op      784 B/op      8 allocs/op
BenchmarkM038IncrementalGroupCountSum-32  2402396      499.6 ns/op      784 B/op      8 allocs/op
BenchmarkM038IncrementalGroupCountSum-32  2446328      485.3 ns/op      784 B/op      8 allocs/op
BenchmarkM038IncrementalGroupCountSum-32  2427902      488.2 ns/op      784 B/op      8 allocs/op
```

Median comparison:

| Path | Time | Bytes | Allocs | Relative to rebuild |
| --- | ---: | ---: | ---: | ---: |
| Rebuild | 4,796,826 ns/op | 8,260,217 B/op | 54,123 | 1.00x |
| Incremental one-row update | 499.6 ns/op | 784 B/op | 8 | 9,601x faster, 10,536x lower bytes, 6,765x fewer allocs |

The result covers a warm retained operator and one update. Initial loading and
full snapshots are separate workloads. Retained memory is proportional to the
number of live groups, and snapshots sort group keys, so those costs should be
measured for cold-start or export-heavy usage.

## Inspiration

This extends the Materialize-style differential aggregation slice after the
retained COUNT operator. It reuses the existing checked arithmetic and callback
contracts from `GroupCountSumInt64DifferentialRows` without changing that batch
API.
