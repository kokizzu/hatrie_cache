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

## Batched same-position COUNT(DISTINCT) and AVG updates

The opt-in mutable numeric RANGE maintainer now has a stable-position fast path
for `COUNT(DISTINCT int64)` and `AVG(int64)`. It validates every update before
publication, retains each validated numeric value in the mutable entry, and
recomputes only output frames containing one of the changed order positions.
`COUNT(DISTINCT)` continues to ignore NULL values and count each remaining value
once. `AVG` continues to ignore NULL values and uses checked `int64` sums before
converting to `float64`. Existing rebuild behavior remains for inserts, deletes,
position or partition changes, mixed operations, duplicate keys, and invalid
values. The append-only constructor and defaults are unchanged.

Run the optimized benchmark with:

```text
make benchmark-m065af-mutable-range-aggregate-batch
```

Matched five-sample medians use 2,000 rows, one partition, a 64-unit frame,
and two same-position updates per operation:

| Kind | Parent fallback | M065af fast path | Improvement |
| --- | --- | --- | --- |
| `COUNT(DISTINCT int64)` | 6,658,542 ns; 3,634,962 B; 40,155 allocs | 1,417,661 ns; 322,257 B; 2,247 allocs | 4.70x faster; 11.28x lower heap; 17.87x fewer allocs |
| `AVG(int64)` | 6,654,410 ns; 3,721,772 B; 41,994 allocs | 1,052,390 ns; 327,635 B; 2,394 allocs | 6.32x faster; 11.36x lower heap; 17.54x fewer allocs |

The cached value fields retain 16 bytes per mutable range entry on the 64-bit
test platform. This feature is opt-in with `NewMutableIncrementalRangeWindow`;
the faster update path therefore trades a small retained-state increase for a
much larger reduction in per-update work and transient allocation.

Raw parent samples (`ns/op`, `B/op`, `allocs/op`), `COUNT(DISTINCT)` first:

```text
6794931 3634996 40154
6752739 3634945 40154
6695959 3635024 40155
6476539 3634962 40155
6658542 3634961 40155

6930399 3721855 41993
6671674 3721841 41999
5461463 3721751 41994
6529930 3721661 41988
6654410 3721772 41995
```

Raw M065af samples, `COUNT(DISTINCT)` first:

```text
1457658 322254 2247
1417661 322258 2248
1304933 322267 2248
1419971 322250 2247
1357748 322257 2247

1035296 327676 2396
1052390 327521 2386
1014107 327635 2394
1088752 327588 2391
1061798 327672 2396
```
