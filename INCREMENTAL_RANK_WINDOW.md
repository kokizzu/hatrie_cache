# Incremental Rank Windows

`hatSql.IncrementalRankWindow` is the rank-window portion of the
Materialize-inspired M065 idea. It maintains one `ROW_NUMBER`, `RANK`, or
`DENSE_RANK` value per partition and emits ordinary `DifferentialRow` updates
for a materialized view or downstream arrangement.

The default `NewIncrementalRankWindow` constructor is append-only and keeps
only counters, partition tails, and row identities. The opt-in
`NewMutableIncrementalRankWindow` constructor retains base rows and supports
arbitrary `INSERT`, `UPDATE`, and `DELETE` mutations with signed retractions.

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

## Mutable Example

```go
window, err := hatSql.NewMutableIncrementalRankWindow(hatSql.IncrementalRankWindowDefinition{
    Kind:         hatSql.IncrementalWindowRowNumber,
    OutputColumn: "row_number",
    PartitionKey: func(row hatSql.Row) (string, error) { return row["team"].(string), nil },
    OrderKey:     func(row hatSql.Row) (interface{}, error) { return row["score"], nil },
    RowKey:       func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
})
_, err = window.Append([]hatSql.Row{
    {"id": "a", "team": "core", "score": int64(10)},
    {"id": "b", "team": "core", "score": int64(20)},
})
updates, err := window.Apply([]hatSql.IncrementalRankWindowMutation{
    {
        Kind: hatSql.IncrementalRankWindowUpdate,
        Key:  "b",
        Row:  hatSql.Row{"id": "b", "team": "core", "score": int64(5)},
    },
    {Kind: hatSql.IncrementalRankWindowDelete, Key: "a"},
})
```

`Apply` emits a negative row for each previous derived value and a positive
row for each replacement value. An ordered insert-only batch uses the same
fast path as `Append`. An unordered insert or a batch containing an update or
delete rebuilds only the affected old and new partitions, then publishes the
new state atomically.

## Contract

- Rows must be non-decreasing within each partition; an out-of-order row is
  rejected with `ErrIncrementalWindowOutOfOrder`.
- Every row key must be stable and unique. Duplicate appends are rejected.
- The complete input batch is validated before any state is published.
  Callback failures, duplicate keys, order violations, and output-column
  collisions leave the window unchanged.
- The append-only maintainer stores rank counters, the last order value per
  partition, and row identities. It intentionally does not retain full input
  rows.
- The mutable constructor additionally retains shallow-copied base rows,
  derived outputs, and the partition for each row. This storage cost is
  explicit and opt-in; the default constructor does not allocate these maps.
- `Apply` validates the full mutation batch before publishing. Keys must be
  unique within a batch, `INSERT` keys must be new, `UPDATE` and `DELETE` keys
  must exist, and an insert/update row key must equal the mutation key.
- Mutable rebuilds use the row key as a deterministic tie-breaker. This makes
  `ROW_NUMBER` stable after arbitrary changes; callers should not rely on
  append arrival order for ties in mutable mode.
- `LAG`/`LEAD`, framed aggregates, and other window functions remain outside
  this narrow rank primitive. Those cases continue using the existing general
  SQL executor or differential operators.

This provides a Materialize-style update/retraction boundary without changing
the existing default SQL result semantics. A storage-owned ordered index could
reduce the remaining sort work for very large affected partitions.

## Benchmark

The existing append benchmark is retained for M065a. Workload: 1,024 rows
split across 16 ordered partitions. The baseline runs the existing
`ROW_NUMBER` SQL query over all 1,024 rows. The append path seeds the same rows
once, then processes one ordered tail row per operation.

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

### Mutable Update Benchmark

Command: `make benchmark-m065-rank-window-mutations`.

This run compares a single update in one of 16 partitions against the parent
full SQL recompute. Five samples were run on Linux/amd64. `B/op` is transient
allocation measured inside the timed operation; the mutable constructor's
retained row maps are seeded outside the timer and are intentionally opt-in.

| Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Parent full SQL recompute | 907,724; 941,029; 915,992; 881,533; 882,566 | 907,724 | 900,019 | 4,653 |
| Mutable affected-partition update | 360,232; 345,045; 354,094; 354,421; 379,028 | 354,094 | 314,476 | 1,066 |

Against the parent full recompute, mutable update maintenance is `2.56x`
faster, uses `2.86x` fewer transient bytes, and makes `4.36x` fewer
allocations. The final append-only control measured `1,018 ns/op`, `863 B/op`,
and `9 allocs/op`, matching the parent allocation profile and preserving the
cheap default path.

## Verification

```text
make test-m065-rank-window-all
make test-race-m065-rank-window-mutations
make benchmark-m065-rank-window-mutations
make test
```
