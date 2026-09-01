# Typed SQL Tables

`hatSql.TypedTable` is an opt-in in-memory table for fixed schemas that need
compact primitive storage and exact changefeed-driven aggregates. Existing
cache-backed `Row` sources and SQL resolvers keep their current behavior.

Each configured column is stored in a type-specific slice (`string`, `int64`,
`float64`, or `bool`) with a separate validity bitmap. `Upsert` requires one
schema-ordered value per column and records an immutable before/after change.
The table can be passed directly to existing SQL execution as a `CACHE` source
whose key is the schema name.

## Setup

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events", // queried as CACHE('events')
	Columns: []hatSql.TypedTableColumn{
		{Name: "team", Kind: hatSql.TypedTableString},
		{Name: "points", Kind: hatSql.TypedTableInt64},
	},
	ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
		Enabled:        true,
		MaxBytes:       4 << 20,
		MinReads:       2,
		RowsPerSegment: 256,
		AdaptiveSegments: true,
	},
})
if err != nil {
	return err
}

change, err := table.Upsert("event-42", []hatSql.TypedTableValue{
	hatSql.TypedString("blue"),
	hatSql.TypedInt64(5),
})
if err != nil {
	return err
}
_ = change
```

`TypedNull()` represents SQL `NULL`. `Delete` produces a change with `After`
unset. The table uses O(columns) swap-delete, so callers must use SQL `ORDER
BY` when row order matters.

## Exact Delta Aggregates

`TypedTableAggregate` consumes strictly ordered table changes. It maintains
grouped `count` and an optional numeric `sum` without rescanning all table
rows.

```go
aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{
	GroupBy:  []string{"team"},
	SumField: "points",
})
if err != nil {
	return err
}

changes, _, err := table.ChangesAfter(aggregate.Checkpoint(), 256)
if err != nil {
	return err
}
if err := aggregate.Apply(changes); err != nil {
	return err
}
rows := aggregate.Rows() // team, count, sum
_ = rows
```

Changes are idempotent at or below the aggregate checkpoint. A sequence gap is
rejected rather than skipped. Keep a durable aggregate checkpoint alongside
your consumer state before calling `CompactChangesThrough`; if `ChangesAfter`
returns `ErrTypedTableChangesCompacted`, rebuild the aggregate from a trusted
table snapshot.

## Shared Incremental Equi-Joins

`TypedTableJoin` maintains one exact inner equi-join between two distinct typed
tables. It snapshots both inputs at construction, then applies each table's
strictly ordered changes without rescanning either source. `NULL` and floating
point `NaN` keys do not match; `-0` and `0` match. A change sequence gap is
rejected, and an already applied sequence is idempotent.

```go
arrangements, err := hatSql.NewTypedTableJoinArrangements(scores, people)
if err != nil {
	return err
}
lease, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{
	LeftField:  "team",
	RightField: "team",
})
if err != nil {
	return err
}
defer lease.Release()

leftChanges, _, err := scores.ChangesAfter(lease.LeftCheckpoint(), 256)
if err != nil {
	return err
}
if err := lease.ApplyLeft(leftChanges); err != nil {
	return err
}
rightChanges, _, err := people.ChangesAfter(lease.RightCheckpoint(), 256)
if err != nil {
	return err
}
if err := lease.ApplyRight(rightChanges); err != nil {
	return err
}
rows := lease.Rows()
_ = rows
```

`TypedTableJoinArrangements` reference-counts consumers of the same pair of
tables and field definition, so identical readers share one maintained join.
The arrangement is removed after its last `Release`; a released lease rejects
further change application and returns no rows. Concurrent `Release` calls are
safe and exactly one succeeds.

Within one `ApplyLeft` or `ApplyRight` batch, consecutive valid changes for the
same primary key are coalesced to their final state. Each change is still
validated in order and checkpoints advance through every sequence. If a later
change is invalid or has a sequence gap, the valid prefix remains applied just
as it would without coalescing. Single-change calls retain the direct path.

The arrangement stores the current source rows, join-key buckets, and a
factorized structural pair of source keys for every match. It deliberately does
not clone full row values or serialize pair keys per match; `Rows` resolves and
independently clones values only for the caller's returned result. This is a
substantial win for repeated incremental updates, but retained state is still
proportional to result cardinality. Keep it out of the default path and do not
use it for unbounded many-to-many joins unless a separate result-cardinality
limit is enforced by the caller.

## SQL Compatibility

The table implements both `SourceResolver` and `ColumnarSourceResolver`.
Existing queries work unchanged:

```sql
FROM CACHE('events') SELECT team WHERE points = 5
```

The columnar resolver avoids building source row maps for the established
single-source SQL fast path. Unsupported query shapes retain the existing row
resolver behavior.

### Optional Immutable Layout Cache

`ColumnarCache` is disabled by default, so existing typed tables do not retain
SQL layouts or change their read/write memory behavior. When enabled, the table
admits an immutable columnar snapshot only after the same field set has been
read `MinReads` times. Omitted or non-positive enabled values use sane defaults:
`4 MiB` maximum retained layouts, `2` reads before admission, and `256` rows per
numeric segment.

The cache uses a byte-bounded least-recently-used eviction policy. A layout that
does not fit `MaxBytes` remains a normal correct columnar scan. Every successful
`Upsert` or `Delete` clears admitted layouts before the changed row is visible.
Warm layouts implement the existing borrowed-layout, segmented-source,
columnar-preference, and source-version contracts. This lets ordinary SQL reuse
the established condition-cache, numeric segment pruning, and Top-N paths
without changing query results.

`AdaptiveSegments` is disabled by default. When enabled, `RowsPerSegment` is a
maximum rather than a fixed size: a small immutable layout may use a bounded,
smaller power-of-two segment to make selective numeric pruning more precise.
It never chooses a size above the configured maximum. The tradeoff is retained
min/max sidecar metadata, so use it for repeated selective numeric filters or
Top-N queries, not broad scans or layouts that are near `MaxBytes`.

## Tradeoff And Measurement

The default remains the existing cache/row source. Use `TypedTable` only for a
fixed, schema-controlled dataset where its explicit per-row API is acceptable.
The changefeed keeps before/after values until compacted, so its retention must
be sized and managed like a journal.

On an AMD Ryzen 9 5950X, `make benchmark-sql-typed-table` measured the median
of three runs as follows:

| Workload | Time/op | Heap/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Exact aggregate, one changed row | 743 ns | 958 B | 8 |
| Full rescan aggregate, 10,000 rows | 2.82 ms | 4.64 MB | 49,745 |
| Selective SQL query, 10,000 rows, rebuilding layout | 810 us | 652 KB | 19,785 |
| Same selective SQL query, warmed immutable layout | 215 us | 3.8 KB | 28 |
| Incremental join update, 10,000 rows per side and 64 join keys | 15.2 us | 945 B | 7 |
| Full join rebuild after that update | 1.33 s | 634 MB | 3,195,005 |
| Two separate same-key updates, 1,024 matching rows | 197 us | 192 KB | 32 |
| One coalesced batch with those updates | 2.56 us | 344 B | 6 |
| Fixed 256-row numeric segments, selective Top-N over 4,096 rows | 56.9 us | 29.4 KB | 436 |
| Adaptive segments (maximum 256), same Top-N | 44.7 us | 27.8 KB | 244 |

For this repeated-aggregate workload, delta maintenance is approximately
`3,794x` faster, uses `4,843x` less heap, and performs `6,218x` fewer
allocations. Those are workload-specific measurements, not a promise for
ad-hoc SQL queries.

For the repeated selective SQL query, the warmed layout is about `3.8x` faster,
uses about `172x` less allocated heap, and performs about `707x` fewer
allocations. The retained snapshot consumes part of `MaxBytes` and is discarded
on every write, so leave the feature disabled for write-heavy or one-shot query
workloads.

For the intentionally high-fanout join fixture, applying one left-side update
is about `87,800x` faster, uses about `671,000x` less allocated heap, and makes
about `456,000x` fewer allocations than a full rebuild. Factorizing matched rows
and using structural pair keys improves the original join implementation by
about `3.4x` in update time, `36.7x` in allocated heap, and `68x` in
allocations. The structural-key change alone improves the already factorized
version by about `1.6x` in update time, `4.9x` in allocated heap, and `23x` in
allocations. The full rebuild materializes roughly 1.56 million matching pairs;
this demonstrates the incremental benefit, not a promise for a low-cardinality
join.

For two consecutive updates of one key with 1,024 current matches, applying
one batch is about `77x` faster, uses about `557x` less allocated heap, and
makes about `5.3x` fewer allocations than applying each update separately. It
does not add retained state and only changes work inside an explicit batch.

For the selective 4,096-row Top-N fixture, adaptive segments are about `1.27x`
faster, use about `1.06x` less allocated heap, and make about `1.8x` fewer
allocations. It uses 64 numeric segments instead of 16, which retains roughly
`2.3 KB` of additional min/max sidecar metadata for two numeric columns. This
is why the feature is opt-in.

## Verification

```sh
make test-sql-typed-table
make test-race-sql-typed-table
make benchmark-sql-typed-table
make benchmark-sql-typed-table-join
make benchmark-sql-typed-table-join-coalescing
```
