# T234 Early Transaction Conflict Detection

`SQLTransactionOptions.EarlyConflictCheck` is an opt-in guard for long-lived
SQL transactions. When enabled, `Execute` and `Query` compare the transaction
snapshot epoch with the live trie before compiling SQL or staging work. A stale
transaction returns `ErrSQLTransactionConflict` immediately.

The default remains `false`, so existing transactions keep commit-only conflict
detection. `Commit` remains the final atomic guard even when early checking is
enabled because another mutation can race with the check. The check is based on
the existing global mutation epoch: any live mutation invalidates the snapshot;
it does not claim key-level disjoint-write detection.

```go
tx, err := hatCache.BeginSQLTransactionWithOptions(trie, hatCache.SQLTransactionOptions{
	EarlyConflictCheck: true,
})
if err != nil {
	return err
}
defer tx.Rollback()

if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); err != nil {
	if errors.Is(err, hatCache.ErrSQLTransactionConflict) {
		return retryTransaction()
	}
	return err
}
return tx.Commit()
```

## Tradeoff

The option adds one atomic epoch load to each `Execute` and `Query` call. It is
intended for transactions where avoiding wasted compilation, staging, or query
work after a concurrent write matters more than the small cost on successful
operations. The default path stays disabled and retains the existing API and
commit behavior.

## Measurement

Measured on AMD Ryzen 9 5950X, Linux amd64, `GOMAXPROCS=1`, seven samples per
row, `-benchtime=300ms`, `-benchmem`. The benchmark truncates staged rows after
each operation so retained staging does not dominate the result.

| Workload | Mode | Median ns/op | B/op | allocs/op | Relative time |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing execute benchmark | origin/master | 3,345 | 2,952 | 13 | 1.00x |
| Existing execute benchmark | T234 default | 3,130 | 2,952 | 13 | 0.94x |
| Non-stale execute | default | 2,869 | 2,952 | 13 | 1.00x |
| Non-stale execute | early check enabled | 3,161 | 2,952 | 13 | 1.10x |
| Stale execute | default | 3,223 | 2,952 | 13 | 1.00x |
| Stale execute | early check enabled | 13.96 | 0 | 0 | 0.0043x |

The stale workload is approximately 231x faster and eliminates the measured
operation allocations when early checking is enabled. The opt-in successful
path measured approximately 10% slower; callers should enable it when the
avoided stale work is worth that cost. The existing default benchmark showed no
allocation or latency regression in this run.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#t234-early-transaction-conflict-detection).
