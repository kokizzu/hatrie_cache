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
Same-position `SUM(int64)` mutation batches use the same checked-delta path for
all updates in one atomic batch, so overlapping frames are adjusted once per
changed value without rebuilding the partition. A batch containing a position
change or a NULL-state transition falls back to the existing partition rebuild.

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

## Batched same-position MIN/MAX updates

Command: `make benchmark-m065ae-mutable-range-extrema-batch`.

This compares a two-row same-position `MIN(int64)`/`MAX(int64)` update batch
on one 2,000-row partition with a preceding bound of 64. The baseline is the
parent M065ad implementation, which rebuilt the affected partition for the
batch. Five samples used `-benchtime=100x` on Linux/amd64.

| Kind | Full batch rebuild | Batched monotonic-deque fast path | CPU improvement | Bytes reduction | Allocation reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| `MIN(int64)` | 5.964 ms, 3,512,152 B, 34,223 allocs | 0.433 ms, 241,359 B, 1,732 allocs | 13.78x | 14.6x | 19.8x |
| `MAX(int64)` | 5.922 ms, 3,525,443 B, 36,169 allocs | 0.451 ms, 255,907 B, 3,665 allocs | 13.13x | 13.8x | 9.9x |

Raw baseline samples (`ns/op`, `B/op`, `allocs/op`), `MIN` first:

```text
5964387 3512280 34218
6133066 3512139 34223
5713435 3512194 34226
5633827 3512140 34223
6020266 3512152 34223

6266500 3525444 36169
5922247 3525443 36170
5939099 3525523 36175
5658690 3525436 36169
5844652 3525426 36169
```

Raw optimized samples, `MIN` first:

```text
464858 241384 1733
461426 241359 1732
429643 241359 1728
419095 241416 1735
432754 241263 1726

450949 255890 3664
444734 255990 3670
451083 256001 3668
445394 255907 3665
490290 255776 3657
```

The fast path is limited to all-update batches that retain one partition and
each row's order position. NULL values remain ignored by the extrema; invalid
values fail atomically. Structural, cross-partition, duplicate-key, and mixed
operation batches retain the existing affected-partition rebuild path.
