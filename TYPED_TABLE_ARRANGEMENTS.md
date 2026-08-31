# Typed Table Aggregate Arrangements

`hatSql.TypedTableAggregateArrangements` shares one exact incremental
`GROUP BY`/`COUNT`/optional `SUM` state among consumers with the same ordered
aggregate definition. It is an explicit application-level optimization; it
does not alter generic SQL planning or ordinary `TypedTableAggregate` behavior.

## Use

```go
arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
if err != nil {
	return err
}

definition := hatSql.TypedTableAggregateDefinition{
	GroupBy:  []string{"team"},
	SumField: "points",
}
first, err := arrangements.Acquire(definition)
if err != nil {
	return err
}
defer first.Release()

second, err := arrangements.Acquire(definition)
if err != nil {
	return err
}
defer second.Release()

changes, _, err := table.ChangesAfter(first.Checkpoint(), 1024)
if err != nil {
	return err
}
if err := first.Apply(changes); err != nil {
	return err
}

rowsForSecondConsumer := second.Rows()
_ = rowsForSecondConsumer
```

Both leases observe the same ordered checkpoint and result. Replaying a batch
is ignored and a sequence gap is rejected, exactly as with
`TypedTableAggregate`.

## Lifecycle And Limits

- Definitions share state only when their ordered `GROUP BY` fields and
  `SUM` field match exactly. Different aggregate definitions remain separate.
- The registry never scans table rows or applies changes by itself. The caller
  chooses the batch boundary and remains responsible for coordinating
  `TypedTable.CompactChangesThrough` after every consumer is past that point.
- `Release` is idempotence-safe and returns `false` after the first release.
  The shared aggregate is discarded when its final lease is released.
- A released lease cannot be used. Acquire a new lease to start a new
  aggregate state.

## Measured Tradeoff

On an AMD Ryzen 9 5950X, two consumers applying the same 10,000 typed-table
changes had these median results:

| Path | Time | Heap | Allocations |
|---|---:|---:|---:|
| Two independent aggregates | 2.45 ms | 1.40 MiB | 60,184 |
| Two leases sharing one arrangement | 1.21 ms | 724 KiB | 30,185 |
| Improvement | 2.02x faster | 1.98x less | 1.99x fewer |

These results apply when consumers need the same aggregate definition. A
single consumer or different definitions should use `TypedTableAggregate`
directly.

## Verification

```sh
make test-sql-typed-arrangements
make benchmark-sql-typed-arrangements
make verify-sql-typed-arrangements
```
