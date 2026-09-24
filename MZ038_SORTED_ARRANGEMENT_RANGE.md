# MZ-038 Sorted Arrangement Range Reads

This feature adopts the Materialize-style idea of seeking into an ordered
arrangement using its logical key rather than scanning every row. It extends
`hatSql.TypedTableSortedArrangement` with an opt-in `RowsRange` API.

## API

```go
rows, err := arrangement.RowsRange(
    &hatSql.TypedTableSortedArrangementBound{
        Values: []hatSql.TypedTableValue{hatSql.TypedInt64(9000)},
        Inclusive: true,
    },
    &hatSql.TypedTableSortedArrangementBound{
        Values: []hatSql.TypedTableValue{hatSql.TypedInt64(9999)},
        Inclusive: false,
    },
    32,
)
```

Bounds contain one value per arrangement order field, not one value per table
column. Composite arrangements use the same field order, direction, and NULL
ordering as the arrangement. A nil lower or upper bound is unbounded. The
limit is applied after seeking; non-positive limits return an empty snapshot.

`RowsRange` validates bound length and typed values, binary-searches the
ordered key vector, and clones only the returned rows. Existing `Rows` and
`RowsPage` behavior is unchanged. The arrangement remains incrementally
maintained by the existing changefeed path; no default index or planner
behavior changed.

## Correctness

Focused tests cover inclusive and exclusive bounds, composite descending
orders, invalid bounds, bounded result copies, and updates applied after the
arrangement is built. Range reads are snapshots and do not expose internal row
storage.

## Benchmark

Machine: Linux amd64, AMD Ryzen 9 5950X, 10,000 rows, five samples per case.
The full-scan baseline materializes the complete ordered snapshot and filters
the requested value range. The offset baseline already knows the row offset;
it is included to show the cost of value-based seeking against a best-case
caller that has an offset.

| Case | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full snapshot plus filter | 1,408,836 | 1,361,417 | 10,001 | baseline |
| `RowsRange` | 4,336 | 4,480 | 33 | 325x faster than full scan |
| Known-offset `RowsPage` | 3,130 | 4,480 | 33 | 1.39x faster than `RowsRange` |

The range path is therefore a large win when the alternative is scanning for
a value predicate, with identical result allocation to a 32-row page. It is
slightly slower than a caller that already knows the exact offset because the
binary search performs additional comparisons. That tradeoff is isolated to
callers that opt into `RowsRange`; existing code pays no cost.

Raw five-sample output is recorded in
[BENCHMARK.md](BENCHMARK.md#mz-038-sorted-arrangement-range-reads).
