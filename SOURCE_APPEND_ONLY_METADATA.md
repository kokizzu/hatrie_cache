# Append-Only Source Metadata

`hatSql.TypedTable.ChangeMetadata` exposes whether a typed table's history has
contained only successful inserts. A consumer can pass that source-owned hint
to `TypedTableAggregate.ApplyWithMetadata`:

```go
change, err := table.Upsert("event-1", values)
if err != nil {
	return err
}
if err := aggregate.ApplyWithMetadata(
	[]hatSql.TypedTableChange{change},
	table.ChangeMetadata(),
); err != nil {
	return err
}
```

The empty table and an insert-only table report `AppendOnly: true`. A
successful update or delete permanently changes the table's metadata to
`AppendOnly: false`. Failed mutations do not change the hint. The zero
metadata value is conservative and selects the existing general `Apply` path.

When `AppendOnly` is true, `ApplyWithMetadata` calls `ApplyMonotone` directly,
avoiding the per-change classification scan performed by `ApplyAuto`. The
monotone path still validates sequence continuity, before/after shape, row
width, and aggregate errors; a false caller-supplied hint therefore fails
closed instead of applying an update as an insert.

`ApplyAuto` remains useful for arbitrary streams that have no source metadata.
Existing `NewTypedTable`, `Upsert`, `Delete`, and aggregate APIs retain their
previous behavior. Consumers should read metadata after the source mutation
that produced the changes and pass it with that batch.

## Benchmark

Raw `go test` output on an AMD Ryzen 9 5950X, Linux amd64, using 4,096
insert-only changes and five samples per sub-benchmark:

| Path | Samples (ns/op) | Median (ns/op) | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| `ApplyAuto` | 286163, 289049, 285429, 286954, 288522 | 286954 | 1840 | 14 |
| `ApplyWithMetadata` | 288733, 286324, 286416, 282957, 280493 | 286324 | 1840 | 14 |

The median is effectively unchanged (`~1.00x`, about 0.2% faster in this
sample) and memory is identical. The benefit is avoiding a redundant O(n)
classification pass when the source already maintains the append-only fact;
the aggregate update itself dominates this workload.
