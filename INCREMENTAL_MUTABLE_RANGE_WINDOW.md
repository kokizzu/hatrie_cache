# Incremental Mutable Numeric RANGE Windows

`NewMutableIncrementalRangeWindow` is an opt-in Materialize-style maintainer
for numeric `RANGE BETWEEN N PRECEDING AND CURRENT ROW` windows. It supports
the same aggregate kinds as `NewIncrementalRangeWindow`: `COUNT`, `SUM(int64)`,
`MIN(int64)`, `MAX(int64)`, `COUNT(DISTINCT int64)`, and `AVG(int64)`.

```go
window, err := hatSql.NewMutableIncrementalRangeWindow(hatSql.IncrementalRangeWindowDefinition{
    Kind:           hatSql.IncrementalRangeWindowSumInt64,
    OutputColumn:   "range_sum",
    FramePreceding: 60,
    OrderKey:       func(row hatSql.Row) (interface{}, error) { return row["event_time"], nil },
    RowKey:         func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
    ValueKey:       func(row hatSql.Row) (interface{}, error) { return row["amount"], nil },
})

changes, err := window.Apply([]hatSql.IncrementalRangeWindowMutation{
    {Operation: hatSql.IncrementalRangeWindowInsert, Row: row},
    {Operation: hatSql.IncrementalRangeWindowUpdate, Key: "row-2", Row: updatedRow},
    {Operation: hatSql.IncrementalRangeWindowDelete, Key: "row-3"},
})
```

Every batch is validated and computed before publication. A missing key,
duplicate key, callback failure, or row-key mismatch leaves the previous state
unchanged. Structural changes rebuild only affected partitions through the
existing optimized append-only RANGE evaluator. Same-position `COUNT` updates
reuse the current frame membership; same-position `SUM(int64)` updates apply a
checked delta only to rows whose numeric frame contains the changed order.

The existing `NewIncrementalRangeWindow` remains the default append-only,
bounded-state path. The mutable constructor retains every base row, partition
ordering, and current output, so its retained memory is O(rows) and should be
chosen only when retractions are required. On the recorded 2,000-row SUM
update workload, the mutable path was about 283x faster, used 4.58x fewer
transient bytes, and used 5.73x fewer allocations than a full materialized
range scan. The retained O(rows) state is not included in per-operation
`B/op`.

Reproduce the checks and benchmark with:

```text
make test-m065w-mutable-range-window
make verify-m065w-mutable-range-window
make benchmark-m065w-mutable-range-window
```
