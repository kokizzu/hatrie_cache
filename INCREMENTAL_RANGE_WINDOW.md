# Incremental Numeric RANGE Windows

`hatSql.NewIncrementalRangeWindow` adds an opt-in append-only maintainer for
numeric `RANGE BETWEEN N PRECEDING AND CURRENT ROW` frames. It is inspired by
Materialize differential maintenance and ClickHouse range-frame execution.

## Supported Contract

- `IncrementalRangeWindowCount` maintains `COUNT(*)`.
- `IncrementalRangeWindowSumInt64` maintains `SUM(int64)` with SQL-like NULL
  handling and checked overflow.
- `OrderKey` must return an `int64`; `FramePreceding` is a non-negative inclusive
  distance in that order direction.
- Rows must arrive in order within each partition. `Descending` supports the
  reverse direction.
- Equal order keys are peers. A new peer emits a retraction and replacement
  insertion for each previous peer because all peers belong to the same RANGE
  frame.

The existing `IncrementalFrameWindow` remains the lower-retention ROWS-frame
API. This feature does not change defaults or add mutable updates/deletes.

## Example

```go
window, err := hatSql.NewIncrementalRangeWindow(hatSql.IncrementalRangeWindowDefinition{
    Kind:           hatSql.IncrementalRangeWindowSumInt64,
    OutputColumn:   "sum_1d",
    FramePreceding: 86400,
    OrderKey: func(row hatSql.Row) (interface{}, error) {
        return row["event_time"], nil
    },
    RowKey: func(row hatSql.Row) (string, error) {
        return row["id"].(string), nil
    },
    ValueKey: func(row hatSql.Row) (interface{}, error) {
        return row["amount"], nil
    },
})
updates, err := window.Append([]hatSql.Row{
    {"id": "a", "event_time": int64(100), "amount": int64(4)},
    {"id": "b", "event_time": int64(100), "amount": int64(6)},
})
```

The two peer rows finish with `sum_1d = 10`; the second append also includes
the `-1` and `+1` replacement for the first row. Input rows are not mutated.
Validation, callback execution, peer replacements, and arithmetic are atomic.

The maintainer retains active range contributions and snapshots for the
current peer group. It also retains row keys for duplicate detection. The
memory cost is explicit because exact peer replacement cannot be derived after
the peer row has been discarded.

## Measurement

Run:

```text
make benchmark-m065m-incremental-range-window
```

The primary comparison materializes the same differential output shape for a
4,096-row workload with a 64-unit frame. Five samples used
`-benchmem -count=5` on Linux amd64/AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Naive recomputation with output | 7,326,222 | 1,732,908 | 28,208 |
| Incremental RANGE maintainer | 2,525,930 | 2,906,353 | 14,326 |
| Improvement | 2.90x faster | 1.68x higher cumulative bytes | 1.97x fewer allocations |

The byte increase is bounded maintainer state plus peer-safe output handling;
the existing ROWS maintainer and all default SQL paths are unchanged.
## M065n: Incremental `RANGE` `MIN`/`MAX`

The same append-only, peer-aware numeric `RANGE` maintainer also supports
`IncrementalRangeWindowMinInt64` and `IncrementalRangeWindowMaxInt64`.

Configure `ValueKey` to return either an `int64` or `nil`. NULL values are
ignored, and the output is `nil` when the current frame has no non-NULL
values. The implementation uses a monotonic deque, so each valid value is
inserted and removed at most once instead of rescanning the active frame.

The ordering key remains an `int64`, rows must remain monotonic in the
configured direction, and peer rows receive the same frame result. The
maintainer is append-only and does not provide arbitrary updates or deletes.
See `BENCHMARK.md` for the measured CPU, cumulative-byte, and allocation
tradeoffs against materialized MIN evaluation.
## M065o: Incremental `RANGE` `COUNT(DISTINCT int64)`

`IncrementalRangeWindowCountDistinctInt64` maintains peer-aware distinct
counts for the same inclusive numeric `RANGE` frame. `ValueKey` must return an
`int64` or `nil`; NULL values are ignored and an empty/all-NULL frame returns
zero. Each active value has an exact multiplicity count, so duplicate values
are removed only after their last contribution expires.

The implementation remains append-only, requires monotonic `int64` order
keys, supports ascending and descending input, and emits exact retractions and
replacements for late peers with the same order key. It does not provide
arbitrary updates or deletes. The measured materialized comparison and memory
tradeoff are recorded in `BENCHMARK.md`.
## M065p: Incremental `RANGE` `AVG(int64)`

`IncrementalRangeWindowAvgInt64` maintains a peer-aware average over the same
inclusive numeric `RANGE` frame. `ValueKey` must return an `int64` or `nil`;
NULL values are ignored and an empty/all-NULL frame returns `nil`. The state
reuses checked `int64` sum arithmetic plus a valid-value count, and emits a
`float64` average.

The append-only, monotonic-order, ascending/descending, and peer-replacement
constraints remain the same as the other numeric RANGE kinds. Sum overflow is
reported atomically, so a failed append can be retried without losing prior
state. See `BENCHMARK.md` for the measured cumulative-byte tradeoff.
## M065q: Incremental `RANGE` `FIRST_VALUE`/`LAST_VALUE`

`IncrementalRangeBoundaryWindow` adds generic peer-aware numeric RANGE boundary
windows for `IncrementalRangeFirstValue` and `IncrementalRangeLastValue`.
`ValueKey` may return any value, including `nil`, and SQL NULL is respected.

`FIRST_VALUE` maintains a compact queue of active frame values and removes
expired rows from the numeric bound. `LAST_VALUE` tracks only the current peer
group: a later row at the same order key emits exact retractions and
replacements for earlier peers, while a later non-peer cannot change their
RANGE frame. Input remains append-only and monotonic within each partition.
See `BENCHMARK.md` for the FIRST_VALUE measurement.
## M065r: Incremental `RANGE` `NTH_VALUE`

`IncrementalRangeNthValueWindow` maintains a fixed one-based NTH_VALUE position
for an inclusive numeric `RANGE BETWEEN N PRECEDING AND CURRENT ROW` frame.
`OrderKey` must return an `int64`; `ValueKey` may return any value, including
`nil`. The active frame is retained in arrival order, so SQL NULL is a real
position and is not skipped.

Rows must arrive in monotonic order within each partition. Ascending order
keeps rows from `order - N` through the current order; descending order keeps
rows from the current order through `order + N`. Equal-order rows are peers,
so a new peer may change the selected position for every earlier peer. The
maintainer emits exact `-1`/`+1` replacements only for peers whose value
changed, followed by the new row's positive differential.

Callback validation, duplicate-key detection, monotonicity checks, and input
state changes are atomic. The API is append-only and opt-in; arbitrary
updates, deletes, dynamic positions, `IGNORE NULLS`, and SQL planner wiring
remain outside this slice. Active rows and the current peer group's original
rows are retained to make expiry and differential replacement exact.
