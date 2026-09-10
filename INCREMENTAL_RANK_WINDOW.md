# Incremental Rank Windows

`hatSql.IncrementalRankWindow` is the append-only portion of the
Materialize-inspired M065 idea. It maintains one `ROW_NUMBER`, `RANK`, or
`DENSE_RANK` value per partition as ordered rows arrive, and emits ordinary
`DifferentialRow` updates for a materialized view or downstream arrangement.

## Example

```go
window, err := hatSql.NewIncrementalRankWindow(hatSql.IncrementalRankWindowDefinition{
    Kind:         hatSql.IncrementalWindowRank,
    OutputColumn: "rank",
    PartitionKey: func(row hatSql.Row) (string, error) { return row["team"].(string), nil },
    OrderKey:     func(row hatSql.Row) (interface{}, error) { return row["score"], nil },
    RowKey:       func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
})
updates, err := window.Append([]hatSql.Row{
    {"id": "a", "team": "core", "score": int64(10)},
    {"id": "b", "team": "core", "score": int64(10)},
    {"id": "c", "team": "core", "score": int64(20)},
})
```

The emitted derived values are `1`, `1`, and `3`. `Append` clones each row
map, writes the derived column, and returns a positive differential update
with the configured row key. `Descending` reverses the order comparison.

## Contract

- Rows must be non-decreasing within each partition; an out-of-order row is
  rejected with `ErrIncrementalWindowOutOfOrder`.
- Every row key must be stable and unique. Duplicate appends are rejected.
- The complete input batch is validated before any state is published.
  Callback failures, duplicate keys, order violations, and output-column
  collisions leave the window unchanged.
- The maintainer stores rank counters, the last order value per partition, and
  row identities. It intentionally does not retain full input rows.
- Deletes, updates, arbitrary insertion order, and `LAG`/`LEAD` or framed
  aggregates remain outside this narrow append-only primitive. Those cases
  continue using the existing general SQL executor or differential operators.

The full M065 feature would need a storage-owned delta arrangement and
retractions for arbitrary updates. This implementation provides the cheap
tail-ingest path without changing existing SQL result semantics.

## Benchmark

Workload: 1,024 rows split across 16 ordered partitions. The baseline runs the
existing `ROW_NUMBER` SQL query over all 1,024 rows. The incremental path seeds
the same rows once, then processes one ordered tail row per operation.

| Path | Sample ns/op | B/op | allocs/op | Relative |
| --- | --- | ---: | ---: | ---: |
| Full SQL recompute before | 958,885, 986,176, 939,420, 923,662, 1,001,587 | 900,008 | 4,653 | 1x |
| Full SQL recompute after | 930,347, 877,934, 875,743, 872,780, 875,711 | 900,006 | 4,653 | 0.92x |
| Incremental ordered append | 1,016, 1,034, 1,092, 1,126, 1,106 | 863 | 9 | 802x faster than after |

Using medians, the incremental append is about `802x` faster, uses about
`1,043x` fewer transient bytes, and performs about `517x` fewer allocations
than recomputing the full 1,024-row window. The comparison is intentionally
between one new tail update and a full refresh, which is the maintenance
workload this feature targets.

## Verification

```text
make test-m065-rank-window
make benchmark-m065-rank-window
make test
```
