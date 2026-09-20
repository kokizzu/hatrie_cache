# Incremental Mutable Offset Window

`NewMutableIncrementalOffsetWindow` is an opt-in Materialize-style maintainer
for `LAG` and `LEAD` values when the input can receive `INSERT`, `UPDATE`, and
`DELETE` changes.

The existing `NewIncrementalOffsetWindow` constructor remains the default. It
is append-only and retains only bounded offset state. The mutable constructor
retains base rows and current derived outputs so it can emit exact
`DifferentialRow` retractions and insertions.

```go
window, err := hatSql.NewMutableIncrementalOffsetWindow(hatSql.IncrementalOffsetWindowDefinition{
    Direction:    hatSql.IncrementalWindowLag,
    OutputColumn: "previous_value",
    Offset:       1,
    DefaultValue: nil,
    PartitionKey: func(row hatSql.Row) (string, error) { return row["group"].(string), nil },
    OrderKey:     func(row hatSql.Row) (interface{}, error) { return row["sequence"], nil },
    RowKey:       func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
    ValueKey:     func(row hatSql.Row) (interface{}, error) { return row["value"], nil },
})

changes, err := window.Apply([]hatSql.IncrementalOffsetWindowMutation{
    {Operation: hatSql.IncrementalOffsetWindowInsert, Row: row},
    {Operation: hatSql.IncrementalOffsetWindowUpdate, Key: "row-2", Row: updatedRow},
    {Operation: hatSql.IncrementalOffsetWindowDelete, Key: "row-3"},
})
```

`Apply` validates the complete batch before publishing it. A failed callback,
missing key, duplicate key, or row-key mismatch leaves the previous state
unchanged. Same-partition, same-order updates use a no-sort path and only
re-evaluate the updated row plus rows whose offset value depends on it.
Structural inserts, deletes, order changes, and partition moves rebuild only
the touched partitions.

The mutable path has an explicit memory cost: it stores every base row,
partition ordering, and current output. It is therefore not enabled by
`NewIncrementalOffsetWindow` and should be used only when retractions are
needed. On the recorded 10,000-row single-value update workload, the mutable
path was about 65.4x faster than full recomputation, with 5.82x lower
operation allocation bytes and 544x fewer allocations. Its measured transient
operation cost was 724,163 B/op; retained state is workload-dependent and is
not included in that per-operation number.

Reproduce the focused checks and benchmark with:

```text
make test-m065v-mutable-offset-window
make verify-m065v-mutable-offset-window
make benchmark-m065v-mutable-offset-window
```
