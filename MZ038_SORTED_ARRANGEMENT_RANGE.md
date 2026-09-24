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

For repeated pagination over one range, `NewRowsRangeCursor` performs the
boundary search once and returns bounded pages from the same ordered vector:

```go
cursor, err := arrangement.NewRowsRangeCursor(lower, upper)
if err != nil {
	return err
}
for {
	rows, done, err := cursor.NextPage(32)
	if err != nil {
		return err
	}
	consume(rows)
	if done {
		break
	}
}
```

The cursor is a value with no range-sized allocation. It records the source
checkpoint and rejects a page request after the arrangement changes with
`ErrTypedTableSortedArrangementCursorChanged`, so callers never receive a
mixed pre-change/post-change range. Each returned row remains an independent
value copy.

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

For a 999-row range consumed as 32-row pages, the cursor removes the repeated
boundary searches without changing output allocation:

| Case | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Repeated `RowsRange` pages | 153,740 | 139,841 | 1,031 | baseline |
| `RowsRange` cursor pages | 126,269 | 139,840 | 1,031 | 1.22x faster, 17.9% lower time |

The range path is therefore a large win when the alternative is scanning for
a value predicate, with identical result allocation to a 32-row page. It is
slightly slower than a caller that already knows the exact offset because the
binary search performs additional comparisons. That tradeoff is isolated to
callers that opt into `RowsRange`; existing code pays no cost.

Raw five-sample output is recorded in
[BENCHMARK.md](BENCHMARK.md#mz-038-sorted-arrangement-range-reads) and
[BENCHMARK.md](BENCHMARK.md#mz-038-sorted-arrangement-range-cursor).
