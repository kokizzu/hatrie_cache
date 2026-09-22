# T033: MVCC Cooperative Yield

T233 makes the existing private-snapshot transaction model explicitly
cooperative. `hatCache.SQLTransaction.Yield(ctx)` is an opt-in safe point for
long-running application workflows: it checks the transaction and context,
releases the transaction mutex, yields the current goroutine, then rechecks
state before returning.

## Usage

```go
for moreWork {
	if err := transaction.Yield(ctx); err != nil {
		return err
	}
	if _, err := transaction.Execute(
		"INSERT INTO cache (key, value) VALUES ('key', 'value')",
	); err != nil {
		return err
	}
}
```

The transaction uses a private snapshot, so the live cache is not held across
the yield. A canceled context returns `context.Canceled` or its context error
but does not automatically abort the transaction; callers can continue or
call `Rollback`. A configured transaction timeout still closes the transaction
and returns `ErrSQLTransactionTimeout`, matching the existing timeout policy.

`Yield(nil)` uses `context.Background()`. Calling `Yield` after commit or
rollback returns the existing closed-transaction error.

## Cost

On an AMD Ryzen 9 5950X, `make benchmark-t233` ran three 3-second samples:

| Safe point | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Ready context | 144.2 | 0 | 0 |
| Already-canceled context | 20.30 | 0 | 0 |

The method is explicit and has no cost on transactions that do not call it.
It does not implement a new storage engine or change commit conflict policy;
the existing snapshot/epoch behavior remains the MVCC boundary.

## Verification

Tests cover normal yield and continued commit, cancellation without implicit
abort, closed transactions, and timeout expiry. Race and allocation benchmarks
are wired through the Makefile:

```text
make format-t233
make test-t233
make race-t233
make vet-t233
make benchmark-t233
```
