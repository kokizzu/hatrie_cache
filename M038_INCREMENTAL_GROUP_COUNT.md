# M038: Incremental Group Count

`IncrementalGroupCount` is a retained differential `GROUP BY ... COUNT(*)`
operator. It keeps one `int64` count per group and emits only the changed
aggregate rows when a signed input batch arrives.

## Behavior

- `NewIncrementalGroupCount` requires a group-key callback.
- `Apply` accepts positive inserts and negative retractions.
- A changed group emits a retraction for its old count and an insertion for its
  new count.
- A group entering the result emits only an insertion; a group leaving it emits
  only a retraction.
- Invalid negative counts and integer overflow reject the entire batch without
  changing retained state.
- `Snapshot` returns positive aggregate rows in lexical group-key order.
- The operator is single-writer; callers sharing it between goroutines must
  synchronize access.

## Benchmark

Command:

```text
make benchmark-m038-incremental-group-count
```

Workload: 10,000 retained input rows in 100 groups, followed by one new row in
`group-42`. Rebuild scans and re-aggregates all 10,001 rows on every iteration.
The incremental benchmark initializes the retained operator outside the timed
region and times only the one-row update. Five benchmark samples were run on
an AMD Ryzen 9 5950X.

Raw result:

```text
BenchmarkM038RebuildGroupCount-32          262  4547312 ns/op  9499666 B/op  39842 allocs/op
BenchmarkM038RebuildGroupCount-32          274  4580774 ns/op  9499644 B/op  39841 allocs/op
BenchmarkM038RebuildGroupCount-32          271  4395215 ns/op  9499642 B/op  39841 allocs/op
BenchmarkM038RebuildGroupCount-32          285  4336779 ns/op  9499643 B/op  39841 allocs/op
BenchmarkM038RebuildGroupCount-32          270  4302055 ns/op  9499643 B/op  39841 allocs/op
BenchmarkM038IncrementalGroupCount-32  2802853      422.2 ns/op      768 B/op      6 allocs/op
BenchmarkM038IncrementalGroupCount-32  2769676      411.9 ns/op      768 B/op      6 allocs/op
BenchmarkM038IncrementalGroupCount-32  2905316      415.7 ns/op      768 B/op      6 allocs/op
BenchmarkM038IncrementalGroupCount-32  2778819      426.0 ns/op      768 B/op      6 allocs/op
BenchmarkM038IncrementalGroupCount-32  2881136      437.5 ns/op      768 B/op      6 allocs/op
```

Median comparison:

| Path | Time | Bytes | Allocs | Relative to rebuild |
| --- | ---: | ---: | ---: | ---: |
| Rebuild | 4,395,215 ns/op | 9,499,643 B/op | 39,841 | 1.00x |
| Incremental one-row update | 422.2 ns/op | 768 B/op | 6 | 10,410x faster, 12,369x lower bytes, 6,640x fewer allocs |

The result is specifically for a warm retained operator and a single update.
Initialization and a full snapshot are separate workloads. The retained map
uses memory proportional to the number of live groups, and `Snapshot` sorts
group keys, so those costs should be measured for cold-start and export-heavy
workloads rather than inferred from the update benchmark.

## Inspiration

This is the first stateful aggregate slice from the Materialize-style
differential-dataflow backlog. It complements the existing batch helpers
`GroupCountDifferentialRows` and `GroupCountSumInt64DifferentialRows` without
changing their APIs or semantics.
