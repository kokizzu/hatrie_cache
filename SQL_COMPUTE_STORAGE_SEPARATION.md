# Optional SQL Compute Pool

MZ-018 is partially adopted as an opt-in compute admission boundary for
managed SQL queries. It is inspired by Materialize's separation of compute
workers from maintained state and by ClickHouse's explicit query worker
controls.

`SQLQueryManager` can execute managed queries on a bounded
`hatPipeline.WorkStealingPool`. The resolver and snapshot are still supplied
by the caller, so this is not a distributed compute/storage split and it does
not move durable writes into a separate process.

## Configuration

The default is unchanged:

```go
manager := hatSql.NewSQLQueryManager(256)
```

Enable an independent query compute pool only when the service wants a fixed
query CPU budget and bounded waiting queue:

```go
manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
	HistoryCapacity:      256,
	ComputeWorkers:       4,
	ComputeQueueCapacity: 64,
})
defer manager.Close()
```

`ComputeWorkers: 0` is the default and keeps execution in the caller's
goroutine. When workers are positive, a zero `ComputeQueueCapacity` selects
`DefaultSQLQueryManagerComputeQueueCapacity` (`64`). The queue counts waiting
queries; up to `ComputeWorkers` queries may also be running.

Use `ValidateSQLQueryManagerOptions` during service configuration loading. The
current hard limits are `MaxSQLQueryManagerComputeWorkers` (`256`) and
`MaxSQLQueryManagerComputeQueueCapacity` (`100000`). Invalid settings are
returned by later `Execute` calls because the existing constructor API returns
only a manager for compatibility.

`Close` rejects new managed queries and drains admitted work. It is idempotent.
It does not cancel a running query, so callers should cancel their request
contexts when shutdown must finish promptly.

## Interaction With Query Workers

`SQLQueryManagerOptions.ComputeWorkers` limits concurrent managed queries.
`SQLQueryOptions.Workers` remains the per-query operator parallelism setting.
Using both can multiply goroutine and CPU demand, so keep the product within
the host's CPU and memory budget.

The compute pool retains no SQL text, source names, parameters, or result rows
itself. A bounded queue does retain the task closure and its caller-owned
references until the task runs or admission is canceled.

## Benchmark Tradeoff

The benchmark uses a deterministic one-row `CACHE` query and a static resolver,
with `go test -cpu 32 -benchmem -count=5` on `linux/amd64` and an AMD Ryzen 9
5950X. The pool is deliberately measured against the existing manager path.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative to legacy concurrent |
| --- | ---: | ---: | ---: | ---: |
| Legacy manager, concurrent callers | 3,597 | 3,506 | 23 | `1.00x` |
| Four-worker compute pool, concurrent callers | 3,982 | 4,419 | 30 | `1.11x` time, `1.26x` memory |
| One-worker compute pool, sequential caller | 5,751 | 4,400 | 30 | use only for isolation |

The isolated pool is therefore useful for admission control and predictable
resource ownership, not for accelerating tiny queries. It remains default-off
because the measured queue, closure, and synchronization cost is not justified
for ordinary low-contention execution.

See the raw samples and baseline comparison in
[BENCHMARK.md#mz-018-optional-sql-compute-pool](BENCHMARK.md#mz-018-optional-sql-compute-pool).
