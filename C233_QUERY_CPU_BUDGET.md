# C233 Query CPU-Time Budget

`SQLQueryOptions.MaxCPUTime` adds an opt-in cooperative execution budget for a
SQL query. The default is `0`, which keeps the existing behavior and does not
sample CPU time.

```go
result, err := hatSql.ExecuteSQLQueryContext(
	ctx,
	query,
	resolver,
	hatSql.SQLQueryOptions{MaxCPUTime: 25 * time.Millisecond},
)
if errors.Is(err, hatSql.ErrSQLQueryCPUTimeExceeded) {
	// Abort, retry, or return a bounded-workload error to the caller.
}
```

## Semantics

- The budget is cooperative, not a preemptive interrupt. Operators check the
  shared execution control at bounded checkpoints and return an error when the
  budget is exceeded.
- The check is active across ordinary materialized scans and projections, in
  addition to existing join, aggregate, sort, and source checkpoints.
- On Linux, the implementation attempts to measure the current thread's user
  and system CPU time. When thread CPU accounting is unavailable or cannot be
  used, it falls back to monotonic elapsed time so a query still receives a
  bounded cancellation signal.
- A resolver or custom function that blocks without returning to a checkpoint
  cannot be interrupted by this option. Use the existing context deadline for
  external blocking work.
- Negative budgets are rejected during query-option validation. Zero disables
  the feature. The exported `ErrSQLQueryCPUTimeExceeded` sentinel can be used
  with `errors.Is`.

## Verification

The focused regression test covers negative-option validation, the default-off
path, and cancellation during a 100,000-row materialized projection:

```text
make test-c233-query-cpu-budget
make race-c233-query-cpu-budget
make vet-c233-query-cpu-budget
```

The raw benchmark and tradeoff are recorded in
[BENCHMARK.md](BENCHMARK.md#c233-query-cpu-time-budget).
