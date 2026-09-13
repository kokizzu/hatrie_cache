# Operator Yield Budgets

This is the MZ-039 adoption from Materialize-style cooperative operator
scheduling. It adds `hatSql.SQLQueryOptions.OperatorYieldEvery`, an explicit
fairness quantum for long SQL/dataflow operators.

## Behavior

`OperatorYieldEvery` counts existing SQL execution-control checks. When the
counter reaches the configured value, the current goroutine calls
`runtime.Gosched()` and the fuel counter is refilled. Native dataflow operators
use the same control through their existing `ctx.Err()` checks, so the native
fast path and the general executor share the setting.

The default is `0`, which disables yielding and preserves the previous
context-check-only behavior. This is a scheduler fairness control, not a total
query-work limit and not preemption: a custom function that does not return to
the SQL executor cannot be interrupted inside its own call. Cancellation still
uses the normal context path.

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, source, resolver,
	hatSql.SQLQueryOptions{
		OperatorYieldEvery: 1024,
	})
```

Use a lower value when query workers must yield more frequently under load.
`1024` is the measured practical starting point. Use `0` for the existing
lowest-overhead behavior. Values above `MaxSQLOperatorYieldEvery` are rejected.

The setting is opt-in and is not automatically applied by namespace or query
defaults. It does not change result ordering, row limits, cancellation errors,
or SQL semantics.

## Measurement

Measured on Linux `amd64`, AMD Ryzen 9 5950X, five samples, with
`-benchtime=200ms -benchmem`. The workload is the native scalar dataflow path
over 4,096 rows. `yields/op` is the observed cooperative yield count.

| Workload | Median ns/op | B/op | Allocs/op | Yields/op | Tradeoff |
| --- | ---: | ---: | ---: | ---: | --- |
| Default, disabled (`0`) | 1,376,788 | 1,409,630 | 8,199 | 0 | control |
| Quantum `64` | 1,504,495 | 1,409,651 | 8,200 | 64.03 | 9.27% slower CPU |
| Quantum `1024` | 1,391,944 | 1,409,652 | 8,200 | 4.00 | 1.10% slower CPU |

The feature does not claim a throughput win. Its improvement is bounded
operator fairness, and its cost is explicit, configurable, and off by default.
Heap and allocation behavior stayed effectively unchanged in this workload.

Raw output from `make benchmark-mz039-after`:

```text
BenchmarkMZ039SQLDataflowBaseline-32 165 1414858 ns/op 0 yields/op 1409630 B/op 8199 allocs/op
BenchmarkMZ039SQLDataflowBaseline-32 169 1363507 ns/op 0 yields/op 1409629 B/op 8199 allocs/op
BenchmarkMZ039SQLDataflowBaseline-32 162 1492725 ns/op 0 yields/op 1409631 B/op 8199 allocs/op
BenchmarkMZ039SQLDataflowBaseline-32 164 1351553 ns/op 0 yields/op 1409667 B/op 8199 allocs/op
BenchmarkMZ039SQLDataflowBaseline-32 170 1376788 ns/op 0 yields/op 1409630 B/op 8199 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery64-32 163 1580127 ns/op 64.03 yields/op 1409650 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery64-32 139 1546365 ns/op 64.03 yields/op 1409652 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery64-32 157 1464381 ns/op 64.03 yields/op 1409651 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery64-32 164 1484666 ns/op 64.03 yields/op 1409651 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery64-32 151 1504495 ns/op 64.03 yields/op 1409651 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery1024-32 166 1391944 ns/op 4.000 yields/op 1409652 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery1024-32 159 1428950 ns/op 4.000 yields/op 1409651 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery1024-32 164 1425295 ns/op 4.000 yields/op 1409653 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery1024-32 171 1352787 ns/op 4.000 yields/op 1409653 B/op 8200 allocs/op
BenchmarkMZ039SQLDataflowYieldEvery1024-32 166 1348512 ns/op 4.000 yields/op 1409651 B/op 8200 allocs/op
```

The repeatable commands are `make benchmark-mz039-before` for the baseline
control and `make benchmark-mz039-after` for all three paired workloads.
