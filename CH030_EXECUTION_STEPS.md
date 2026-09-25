# CH-030 Execution-Step Budget

CH-030 is partially adopted through an opt-in query work budget. Existing
row, join, result, grouping, spill, and timeout limits remain available; this
adds a bound on the executor's cooperative execution checks.

## API

```go
result, err := hatSql.ExecuteSQLQueryParameters(
	ctx,
	query,
	resolver,
	parameters,
	hatSql.SQLQueryOptions{MaxExecutionSteps: 100000},
)
if errors.Is(err, hatSql.ErrSQLExecutionStepsExceeded) {
	// Abort or retry with a larger workload budget.
}
```

`MaxExecutionSteps == 0` preserves the existing unbounded behavior. A
positive value increments at the executor's centralized cooperative checks;
exceeding it returns `ErrSQLExecutionStepsExceeded`. This is a bounded CPU
work proxy, not a CPU-cycle counter: custom work that does not return to the
executor cannot be interrupted by this budget. Use `Timeout` for wall-clock
deadlines.

Queries with a nonzero execution-step budget are excluded from the complete
SQL result cache, so a cached result cannot bypass the requested limit.

## Measurement

Linux/amd64, AMD Ryzen 9 5950X, five 500 ms samples per benchmark.

| Path | Median time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Existing central check, before change | 3.763 ns/op | 0 B/op | 0 |
| Default central check, after change | 4.320 ns/op | 0 B/op | 0 |
| Enabled central check | 4.259 ns/op | 0 B/op | 0 |
| Existing representative query, before change | 5,386 ns/op | 5,952 B/op | 32 |
| Default representative query, after change | 5,451 ns/op | 5,952 B/op | 32 |
| Bounded representative query | 5,124 ns/op | 5,952 B/op | 32 |

The query-level difference is within normal benchmark variance and the
feature adds no measured allocations. The direct check benchmark shows a
sub-nanosecond absolute change; the budget remains opt-in.

## Verification

- `make test-ch030-execution-steps`
- `make benchmark-ch030-execution-steps-baseline`
- `make benchmark-ch030-execution-steps`
- `make verify-ch030-execution-steps`
