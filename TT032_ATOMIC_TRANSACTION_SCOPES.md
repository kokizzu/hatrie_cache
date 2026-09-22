# T032: Atomic Transaction Scopes

T232 adds Tarantool-style nested transaction scopes to the importable
`hatCache.SQLTransaction` API. A scope is a rollback boundary inside an open
transaction: successful scope work remains staged in the parent transaction,
while a callback error rolls back only the nested work.

## Callback API

```go
err := transaction.Scope(func(transaction *hatCache.SQLTransaction) error {
	if _, err := transaction.Execute(
		"INSERT INTO cache (key, value) VALUES ('temporary', 'value')",
	); err != nil {
		return err
	}
	return errors.New("discard this scope")
})
```

The callback error is returned, the temporary write is discarded, and the
parent transaction remains usable. Scopes can be nested; an inner error can be
handled by the outer callback while outer writes remain staged.

For explicit lifecycle control, `BeginScope` returns an
`SQLTransactionScope`:

```go
scope, err := transaction.BeginScope()
if err != nil {
	return err
}
// Stage writes in transaction.
if err := scope.Commit(); err != nil {
	return err
}
```

`scope.Rollback()` discards only the scope boundary. `Scope` also rolls back
on a callback panic and re-panics after cleanup. The outer transaction still
needs `Commit()` to publish its staged writes.

## Cost And Limits

Scopes reuse the existing SQL savepoint implementation. Each scope boundary
therefore clones the private snapshot and uses temporary snapshot storage;
this is deliberately opt-in and does not affect transactions that do not use
scopes. Use a scope when nested failure isolation is more important than the
snapshot-clone cost.

In a low-contention run on an AMD Ryzen 9 5950X, `make benchmark-t232` ran
three 3-second samples:

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Manual `Savepoint` + `RollbackTo` + `ReleaseSavepoint` | 7,693,559 | 543,584 | 351 |
| `SQLTransaction.Scope` callback | 7,596,640 | 544,520 | 354 |

The scoped API was effectively CPU-neutral in this run, about 1.3% faster,
with an opt-in difference of 936 B/op and 3 allocations. Repeated runs while
other filesystem-heavy work was active showed wide latency variance, so the
small CPU difference is not treated as a performance claim. The underlying
savepoint itself remains materially more expensive than a transaction without
one because of the snapshot clone; the API is an ergonomics and correctness
improvement, not a claim that nested snapshots are cheap.

## Verification

Focused tests cover successful nested scopes, inner rollback while preserving
outer work, explicit scope lifecycle, callback errors, callback panics,
reserved-name collision handling, and post-rollback transaction reuse.

```text
make format-t232
make test-t232
make race-t232
make vet-t232
make benchmark-t232
```
