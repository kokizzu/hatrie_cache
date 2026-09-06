# Monotone Aggregate Path

`TypedTableAggregate.ApplyMonotone` is an opt-in specialization for a
contiguous insert-only typed-table change feed. It rejects `Before` values and
missing `After` values before changing aggregate state, ignores already
replayed sequence numbers, and preserves the normal sequence and overflow
contracts.

```go
if err := aggregate.ApplyMonotone([]hatSql.TypedTableChange{
	{Sequence: 1, After: values},
}); err != nil {
	return err
}
```

For count/sum-only aggregates it uses the direct monotone group update path.
Aggregates with min, max, or distinct fields retain the established typed
aggregate semantics through the same append-only entry point. Deletes and
updates must use `Apply` instead.

## Measurement

On an AMD Ryzen 9 5950X, three samples of the repository benchmark produced:

| Path | Median time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| `ApplyMonotone` | `76.4 us/op` | `560 B/op` | `7 allocs/op` |
| General insert-only `Apply` | `83.3 us/op` | `560 B/op` | `7 allocs/op` |

The measured path was about `1.09x` faster. This is a narrow CPU win; it does
not change the default aggregate path or claim a benefit for mixed mutations.
