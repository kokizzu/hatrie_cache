# M211 SQL Frontier Bounds

M211 makes retained SQL history an explicit half-open frontier interval:
`[since, upper)`. A request before `since` or at/after `upper` is rejected
before a snapshot is opened, so callers cannot receive a partial or ambiguous
historical view.

## API

Providers that can report retained bounds implement the optional
`SQLFrontierBoundsProvider` interface:

```go
bounds, err := table.SQLFrontierBounds()
if err != nil {
	return err
}
if err := bounds.Validate(frontier); err != nil {
	return err
}
```

`TypedTable` reports `Since` from MVCC compaction and `Upper` as one past its
latest committed sequence. `TypedTableSQLSnapshotRegistry` reports the
intersection of all registered table intervals, so every table in a shared
snapshot is valid at the same frontier.

The common `AS OF`, `BeginSQLFrontierSnapshot`, and
`BeginSQLDistributedFrontierSnapshot` paths validate bounds when the optional
provider is present. The sentinels are `ErrSQLFrontierBeforeSince`,
`ErrSQLFrontierAtOrAfterUpper`, and `ErrSQLFrontierBoundsInvalid`.

Providers that do not implement the optional interface retain the previous
unbounded behavior. Ordinary live reads and the MVCC-disabled default are
unchanged.

## Benchmark

Linux/amd64 on AMD Ryzen 9 5950X, five `-count=5` samples. The baseline calls
the existing raw `SnapshotAt` path on the same 256-row fixture.

| Workload | Samples (ns/op) | Median ns/op | B/op | Allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Raw `SnapshotAt` baseline | 12969; 12471; 14416; 14723; 15728 | 14416 | 18952 | 7 | 1.00x |
| Bounds-checked `BeginSQLSnapshotAt` | 14573; 14727; 14365; 14510; 13635 | 14510 | 18952 | 7 | 1.01x CPU; same bytes/allocs |
| `SQLFrontierBounds.Validate` only | 2.882; 2.889; 2.864; 2.967; 3.147 | 2.889 | 0 | 0 | zero-allocation validation |

The 1.01x CPU difference is the small cost of the correctness check; M211 is
not a performance claim. The measured bounds check adds no allocation or
retained memory.

Run the measurements with:

```text
make benchmark-m211-frontier-bounds-baseline
make benchmark-m211-frontier-bounds
```
