# Incremental Sort Arrangement

`hatSql.NewTypedTableSortedArrangement` maintains a deterministic ordered
view of a typed table:

```go
arrangement, err := hatSql.NewTypedTableSortedArrangement(
	table,
	hatSql.TypedTableSortedArrangementDefinition{
		Field: "updated_at",
	},
)
if err != nil {
	return err
}

change, err := table.Upsert("row-1", values)
if err != nil {
	return err
}
if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
	return err
}
ordered := arrangement.Rows()
```

The arrangement uses binary-search insertion and ordered-slice shifts for
small updates. Contiguous batches of at least 64 valid changes rebuild the
order once, avoiding one shift per change. Existing table and SQL behavior is
unchanged; callers explicitly opt into the arrangement when they need a
reusable ordered view for `ORDER BY`-like access.

## Contract

- `Field` must name a typed-table column.
- `Descending` reverses non-null values. `NullsFirst` controls NULL and NaN
  placement independently of direction.
- Equal sort values are ordered by the source row key, making snapshots
  deterministic.
- Inserts, updates, and deletes must follow contiguous source sequences.
  Replays are ignored; a gap applies the valid prefix and returns an error.
- Rows are independently cloned by `Rows`; the arrangement never exposes its
  internal value slices.
- The source table is snapshotted at construction. `Apply` advances it from
  that checkpoint and does not read later source state implicitly.

Use the arrangement for frequent point updates and repeated ordered reads. For
large batches, a source-specific full rebuild can be faster; the implementation
already uses one bulk rebuild for large contiguous batches, but it still keeps
its map-backed arrangement state and clone ownership guarantees.

## Benchmark

Raw `go test` output on an AMD Ryzen 9 5950X, Linux amd64, with 4,096 rows and
five samples per benchmark. The single-update case changes one row; the batch
case changes 256 rows and reads the resulting checkpoint. The rebuild baseline
applies the same row updates to a slice and sorts it.

| Workload | Incremental samples (ns/op) | Incremental median | Rebuild samples (ns/op) | Rebuild median |
| --- | --- | ---: | --- | ---: |
| One update | 790.1, 769.3, 791.0, 782.8, 780.6 | 782.8 | 133596, 137742, 133532, 131868, 132616 | 133532 |
| 256-update batch | 594473, 584731, 590211, 588598, 579678 | 588598 | 138476, 137770, 137476, 139393, 140257 | 138476 |

For one update, the incremental path is approximately **170x faster**, uses
**1.25x fewer measured bytes** (`96` vs `120`), and performs **3x fewer
allocations** (`1` vs `3`). For the 256-update batch it is approximately
**4.25x slower** (`407019` vs `120` measured bytes/op; `278` vs `3` allocs/op),
so large batch consumers should benchmark their workload before selecting it.
The batch byte/allocation comparison is directional: the incremental path
clones update values and maintains its reusable map-backed state, while the
minimal rebuild fixture updates an in-place slice.
