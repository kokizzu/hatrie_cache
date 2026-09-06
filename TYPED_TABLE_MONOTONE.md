# Monotone Typed-Table Aggregates

`TypedTableAggregate.ApplyMonotone` is an append-only specialization for
sources that produce only contiguous insert changes. Each change must have:

- a sequence equal to the aggregate checkpoint plus one;
- no `Before` values;
- non-empty `After` values.

Sequences at or below the checkpoint are treated as replays and ignored, just
like `Apply`. Gaps, deletes, and empty changes are rejected before aggregate
state is changed. Existing `Apply` behavior is unchanged.

```go
aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{
	GroupBy:  []string{"team"},
	SumField: "points",
})
if err != nil {
	panic(err)
}
if err := aggregate.ApplyMonotone(insertChanges); err != nil {
	panic(err)
}
```

For count-only and count-plus-sum definitions, the method uses a specialized
positive-update path. Definitions that also maintain MIN, MAX, or COUNT
DISTINCT use the existing general row updater so their exact delete-aware
bookkeeping remains available for future mixed input.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-typed-table-monotone
```

The benchmark applies 1,024 insert changes to a count-plus-sum aggregate on an
AMD Ryzen 9 5950X development machine:

| Path | Time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| `ApplyMonotone` fast path | 69.2-70.8 us/op | 560 B/op | 7 allocs/op |
| General `Apply` | 72.3-78.9 us/op | 560 B/op | 7 allocs/op |

The median is approximately `1.10x` faster with no measured allocation or
memory increase. This is a focused in-process benchmark; source decoding and
table writes are excluded.
