# Distributed Partial Aggregation

`hatSql.TypedTableAggregate` can merge completed partition-local aggregate
state with `MergePartial` or `MergePartials`. This is useful when each region,
partition, or worker aggregates its own input and a final worker combines the
small number of partial states instead of replaying every source change.

## Supported State

The merge is exact for the aggregate state currently maintained by
`TypedTableAggregate`:

- `COUNT`, which is always maintained for every group;
- `SUM`, when `SumField` is configured;
- `MIN`, when `MinField` is configured;
- `MAX`, when `MaxField` is configured; and
- `COUNT DISTINCT`, when `DistinctField` is configured.

Grouped values and their multiplicity maps are copied into the target. The
partial aggregate is never mutated. Floating-point `SUM` has the same normal
IEEE-754 order sensitivity as replaying the source rows in a different order.

## Example

Build one aggregate per partition, then merge them into an empty final
aggregate:

```go
definition := hatSql.TypedTableAggregateDefinition{
	GroupBy:       []string{"region"},
	SumField:      "amount",
	MinField:      "amount",
	MaxField:      "amount",
	DistinctField: "customer",
}

westAggregate, err := hatSql.NewTypedTableAggregate(westTable, definition)
if err != nil {
	return err
}
eastAggregate, err := hatSql.NewTypedTableAggregate(eastTable, definition)
if err != nil {
	return err
}
finalAggregate, err := hatSql.NewTypedTableAggregate(finalTable, definition)
if err != nil {
	return err
}

if err := westAggregate.Apply(westChanges); err != nil {
	return err
}
if err := eastAggregate.Apply(eastChanges); err != nil {
	return err
}
if err := finalAggregate.MergePartials(westAggregate, eastAggregate); err != nil {
	return err
}
```

For input rows `(west, 10, alice)`, `(west, 5, bob)`, and
`(west, 7, alice)` across the partials, `Rows()` contains one row equivalent
to:

```go
hatSql.Row{
	"region":         "west",
	"count":          int64(3),
	"sum":            float64(22),
	"min":            int64(5),
	"max":            int64(10),
	"count_distinct": int64(2),
}
```

## Contract

- `MergePartial(partial)` is shorthand for `MergePartials(partial)`.
- `MergePartials` accepts zero or more completed partials. Zero arguments is a
  no-op.
- Target and partials must have the same table column names, column kinds,
  grouping columns, and configured aggregate fields.
- A nil target or partial returns `ErrTypedTableAggregatePartialNil`.
- Merging an aggregate into itself returns
  `ErrTypedTableAggregatePartialSelf`.
- Incompatible definitions return `ErrTypedTableAggregatePartialDefinition`.
- Validation and count-overflow failures leave the target unchanged.
- Repeatedly merging the same partial counts it repeatedly. The API does not
  deduplicate delivery; callers must use an idempotency key or checkpoint at
  the partition boundary when retrying a merge.
- The final aggregate's source checkpoint is not advanced by a merge. Merge
  only combines aggregate state; source changes and checkpoint persistence
  remain the caller's responsibility.
- Do not mutate a partial concurrently with `MergePartials`. The aggregate
  type itself remains subject to the same synchronization requirements as
  `Apply` and `Rows`.

The API accepts in-process aggregate objects. It does not define a wire or
storage encoding for partials; callers may serialize a partition result using
the repository's selected transport/storage codec and reconstruct an
aggregate before merging. Therefore this feature's benchmark measures CPU
and heap cost of final merging, not network bandwidth.

## Benchmark

Run the reproducible benchmark with:

```text
make benchmark-c093-clean
```

The benchmark uses two 4,096-row partials, 256 groups, and all supported
aggregate state. It compares merging the two precomputed partials with
replaying the combined 8,192 change records on the same machine (AMD Ryzen 9
5950X, Linux/amd64):

| Path | Median ns/op | B/op | allocs/op | Relative latency | Relative bytes | Relative allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `MergePartials` | 1,759,279 | 1,823,491 | 2,578 | 1.00x | 1.00x | 1.00x |
| Replay combined changes | 3,521,376 | 2,222,802 | 3,857 | 2.00x slower | 1.22x higher | 1.50x higher |

The result is a final-merge CPU and heap improvement for this workload. The
larger system-level win is avoiding transfer of every raw change to the final
worker; actual bandwidth depends on the chosen partial-state codec and the
number of groups/distinct values.
