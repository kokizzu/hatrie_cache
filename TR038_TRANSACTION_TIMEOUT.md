# TR-038 Transaction Timeouts

Hatrie Cache now supports an optional wall-clock timeout for SQL transactions.
This follows the bounded-work idea used by Tarantool and streaming SQL systems:
an abandoned transaction must not retain a private snapshot or a serializable
command lock indefinitely.

## Usage

```go
transaction, err := BeginSQLTransactionWithOptions(trie, SQLTransactionOptions{
	Timeout: 5 * time.Second,
})
if err != nil {
	return err
}
defer transaction.Rollback()

if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('job:1', 'ready')"); err != nil {
	if errors.Is(err, ErrSQLTransactionTimeout) {
		return fmt.Errorf("transaction expired: %w", err)
	}
	return err
}
if err := transaction.Commit(); err != nil {
	return err
}
```

The example requires the standard `errors`, `fmt`, and `time` packages.

## Semantics

- `Timeout: 0` is the default and disables the feature. Existing callers keep
  the no-clock-check path.
- A positive timeout starts after the private transaction snapshot has been
  captured.
- `Execute`, `Query`, `Savepoint`, `RollbackTo`, `ReleaseSavepoint`, and
  `Commit` check the deadline at their operation boundaries.
- `Query` derives a context deadline from the transaction timeout, so a long
  relational query is cancelled at the earlier of the caller or transaction
  deadline.
- Expiration closes the transaction, destroys its private snapshot and
  savepoints, and releases a serializable transaction lock. Further calls keep
  returning `ErrSQLTransactionTimeout`.
- `Rollback` remains safe after expiration and is the normal cleanup pattern.
- The timeout is cooperative at API boundaries. It does not spawn a timer
  goroutine or preempt arbitrary Go code. `Commit` checks before publishing its
  atomic batch; an already-running commit is not interrupted halfway through.
- Negative durations are rejected during `BeginSQLTransactionWithOptions`.

`SQLQueryOptions.Timeout` remains the existing per-query/statement timeout.
`SQLTransactionOptions.Timeout` adds a single upper bound across the complete
transaction lifetime.

## Cost

The timeout is intentionally opt-in. On the benchmark host, the disabled guard
measured a 4.227 ns median with 0 B/op and 0 allocs/op. The enabled guard measured
47.74 ns with the same allocation profile, about 11.30x the guard cost. This is
the cost of one mutex-protected deadline check and a monotonic `time.Now` read;
the default path does not call `time.Now`. See [BENCHMARK.md](BENCHMARK.md#tr-038-sql-transaction-timeouts)
for all raw samples.
