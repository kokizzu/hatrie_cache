# TR-037 Deadlock Detection

`SQLRowLockManager` now has an opt-in owner-aware path for multi-key
transactions. `AcquireOwned` and `TryAcquireOwned` associate a stable caller
owner with each lease. When `EnableDeadlockDetection` is true, blocked owners
form a bounded wait-for graph; a cycle returns `ErrSQLRowLockDeadlock` instead
of waiting forever.

```go
manager := hatSql.NewSQLRowLockManager(hatSql.SQLRowLockManagerOptions{
	EnableDeadlockDetection: true,
})

first, err := manager.AcquireOwned(ctx, "transaction-1", "orders/42")
if err != nil {
	return err
}
defer first.Release()

second, err := manager.AcquireOwned(ctx, "transaction-1", "inventory/7")
if err != nil {
	return err
}
defer second.Release()
```

The default is unchanged: `EnableDeadlockDetection` is false, and ordinary
`Acquire`/`TryAcquire` use the existing token-channel path. Owner IDs and wait
edges are bounded by `MaxOwnerBytes` and `MaxWaitEdges`; exceeding the graph
bound returns `ErrSQLRowLockWaitGraphCapacity`. All participants in a
transaction must use the same stable owner ID. Anonymous acquisitions cannot
participate in cycle detection.

This is process-local coordination only. It does not provide distributed
fencing, durable transaction recovery, or automatic SQL `FOR UPDATE` grammar.
Those remain caller-owned, as does a policy for retrying a transaction after a
deadlock error.

## Measured Cost

Five 100 ms samples on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Default row-lock acquire/release | 345.6 | 224 | 3 |
| Owner-aware detection acquire/release | 504.5 | 448 | 5 |
| Contended `TryAcquireOwned` check | 24.12 | 0 | 0 |

The safety path is about 1.46x the default lease cost and adds 224 B plus two
allocations for an uncontended acquire/release. That cost is opt-in; the
default path has no wait-for graph work. See [BENCHMARK.md](BENCHMARK.md#tr-037-deadlock-detection).
