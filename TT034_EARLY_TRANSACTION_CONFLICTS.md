# T034: Early Transaction Conflict Detection

`SQLTransaction` keeps its existing optimistic snapshot behavior by default.
When an application needs stale work rejected earlier, it can opt in with
`SQLTransactionOptions.EarlyConflictDetection`:

```go
transaction, err := hatCache.BeginSQLTransactionWithOptions(cache, hatCache.SQLTransactionOptions{
	EarlyConflictDetection: true,
})
if err != nil {
	return err
}
defer transaction.Rollback()

if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('order-1', 'queued')"); err != nil {
	if errors.Is(err, hatCache.ErrSQLTransactionConflict) {
		return retryTransaction(err)
	}
	return err
}
return transaction.Commit()
```

With the option enabled, each staged mutation checks the live mutation epoch
before applying to the private snapshot and again after that application. A
detected live mutation closes the transaction, discards its private changes,
and returns `ErrSQLTransactionConflict`. The commit path still performs its
normal epoch check because a live mutation can happen after `Execute` returns.

The zero value remains disabled. Default transactions therefore retain their
existing commit-time-only conflict behavior and do not pay the extra atomic
epoch checks. The opt-in mode is useful when work after a stale write would be
expensive or externally visible, while the default mode remains the lower
policy surface for callers that already handle commit conflicts.

## Measurement

On an AMD Ryzen 9 5950X, three 3-second samples compared a transaction that
creates a snapshot and stages one valid `INSERT`. The early-detection sample
enabled the option but did not inject a competing mutation, so it measures the
no-conflict overhead rather than the error path:

| Mode | Median CPU | Heap | Allocations |
| --- | ---: | ---: | ---: |
| Default | `4,230 ns/op` | `4,267 B/op` | `14 allocs/op` |
| Early detection | `4,085 ns/op` | `4,248 B/op` | `14 allocs/op` |

The measured difference is within normal run-to-run noise and is not claimed
as a speedup. The opt-in checks added no measured allocation cost in this
snapshot-dominated workload. Focused tests also verify the actual conflict
path, no stale live write, transaction closure, default late detection, and a
successful no-conflict opt-in commit.
