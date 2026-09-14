# MZ-034 Generic Negative-Diff Operators

## Status

MZ-034 is already substantially implemented as an importable `hatSql`
differential operator surface. It represents relation changes as
`DifferentialRow` values with signed `int64` `Diff` weights, so inserts,
retractions, and duplicate multiplicity can flow through the same operators.

This is a library API, not automatic differential execution for every SQL plan.
The regular SQL evaluator and operator wiring remain unchanged by default, and
MZ-035 remains open for auditing multiset preservation across every path.

## Public Surface

| Capability | API | Behavior |
| --- | --- | --- |
| Change record | `DifferentialRow` | Carries `Key`, logical `Time`, signed `Diff`, and a `Row` payload. |
| Selection | `FilterDifferentialRows` | Preserves non-zero signed weights and duplicate updates for selected rows. |
| Projection | `MapDifferentialRows` | Applies a one-to-one mapping and consolidates equal output key/time identities. |
| Expansion | `FlatMapDifferentialRows` | Applies a one-to-many mapping while inheriting timestamp and signed weight. |
| Union | `UnionDifferentialRows` | Implements `UNION ALL`-style signed concatenation with consolidation. |
| Join | `JoinDifferentialRows` | Multiplies left and right signed weights and uses the later input time. |
| Difference | `NegateDifferentialRows`, `ExceptDifferentialRows` | Reverses or subtracts signed updates while preserving multiplicity. |
| Intersection | `DifferentialIntersect` | Maintains two input multiplicities and emits changes to their minimum. |
| Grouped count/sum | `GroupCountDifferentialRows`, `GroupSumInt64DifferentialRows`, `GroupCountSumInt64DifferentialRows` | Emits exact before/after aggregate transitions and rejects invalid negative counts or overflow. |
| Grouped extrema/average | `GroupMinMaxInt64DifferentialRows`, `GroupAverageInt64DifferentialRows` | Retains enough state to emit exact retractions and replacement values. |
| Temporal join | `DifferentialTemporalJoin` | Applies signed changes to bounded temporal matches. |

The newer stateful operators for incremental Top-K, distinct, and percentile
also use signed differential rows, but are documented separately under MZ-037,
MZ-039, and MZ-040.

## Example

```go
rows := []hatSql.DifferentialRow{
    {Key: "user-1", Time: 10, Diff: 2, Row: hatSql.Row{"active": true}},
    {Key: "user-1", Time: 10, Diff: -1, Row: hatSql.Row{"active": true}},
}

active, err := hatSql.FilterDifferentialRows(rows, func(row hatSql.Row) (bool, error) {
    return row["active"] == true, nil
})
if err != nil {
    return err
}
active, err = hatSql.ConsolidateDifferentialRows(active)
if err != nil {
    return err
}
// active contains one row with Diff=1 after consolidation of equal identity.
_ = active
```

The APIs clone row maps when a callback or output retains them, so callers can
reuse input batches after the call. Zero-diff updates are ignored. Invalid
keys, `math.MinInt64` negation, signed arithmetic overflow, and negative
stateful multiplicities return errors without partial output or state mutation.

## Correctness Coverage

The existing tests cover signed filter/map/flat-map/union/join behavior,
duplicate consolidation, difference overflow, multiset intersection
transitions, grouped aggregate retractions, invalid negative multiplicities,
callback failures, input immutability, and atomic validation. Focused checks:

```text
make test-mz034-c203
make race-mz034-c203
make vet-mz034-c203
make benchmark-mz034-c203
```

## Benchmark

Measurements are in-process Go microbenchmarks on an AMD Ryzen 9 5950X. Each
value below is the median of five samples from
`make benchmark-mz034-c203`; `-benchmem` is enabled. For the two comparative
rows, a value above `1x` means lower CPU time or memory in the current path.
The `INTERSECT` allocation ratio is shown separately because its incremental
path retains more short-lived allocations while avoiding repeated full
rebuilds.

| Workload | Baseline | Current | CPU improvement | Memory improvement | Allocation improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| `EXCEPT` composition vs optimized | 3.002 ms/op | 1.005 ms/op | 2.99x | 2.84x | 4.97x |
| `INTERSECT` rebuild vs incremental | 15.947 ms/op | 0.392 ms/op | 40.65x | 30.97x | 0.63x, or 1.58x more allocations |
| `FILTER` current path | n/a | 56.498 us/op | baseline | 96,896 B/op | 513 allocs/op |
| `MAP` current path | n/a | 142.343 us/op | baseline | 298,280 B/op | 1,541 allocs/op |
| `FLAT MAP` current path | n/a | 150.806 us/op | baseline | 304,424 B/op | 1,797 allocs/op |
| `UNION` current path | n/a | 138.048 us/op | baseline | 256,552 B/op | 1,029 allocs/op |
| `JOIN` current path | n/a | 48.591 us/op | baseline | 56,984 B/op | 274 allocs/op |

Raw samples:

```text
BenchmarkExceptDifferentialRows/BaselineComposition: 3.013601, 3.001982, 3.042189, 2.954546, 2.783616 ms/op; 4,752,008-4,752,023 B/op; 20,515 allocs/op
BenchmarkExceptDifferentialRows/Optimized: 0.935434, 0.987351, 1.037922, 1.004609, 1.022720 ms/op; 1,671,808-1,671,812 B/op; 4,130 allocs/op
BenchmarkDifferentialIntersect/RebuildSnapshot: 15.947372, 15.494768, 15.618948, 16.369395, 16.703262 ms/op; 20,361,737-20,361,746 B/op; 1,796 allocs/op
BenchmarkDifferentialIntersect/Incremental: 0.402216, 0.398049, 0.386791, 0.392333, 0.385605 ms/op; 657,530 B/op; 2,837 allocs/op
BenchmarkDifferentialOperators/filter: 56.269, 58.329, 57.151, 55.915, 56.498 us/op; 96,896 B/op; 513 allocs/op
BenchmarkDifferentialOperators/map: 141.373, 150.424, 141.540, 142.343, 144.667 us/op; 298,280 B/op; 1,541 allocs/op
BenchmarkDifferentialOperators/flat_map: 148.672, 150.806, 152.373, 151.679, 143.599 us/op; 304,424 B/op; 1,797 allocs/op
BenchmarkDifferentialOperators/union: 132.919, 138.048, 140.116, 134.508, 140.493 us/op; 256,552 B/op; 1,029 allocs/op
BenchmarkDifferentialOperators/join: 48.171, 48.896, 48.591, 48.108, 49.245 us/op; 56,984 B/op; 274 allocs/op
```

The incremental intersection path is the clear win for update-heavy workloads,
but its higher allocation count is a real tradeoff. It should not be treated as
a universal replacement for a one-shot rebuild.
