# C233 Per-Query CPU-Time Budgets

SQL execution now has an opt-in cooperative CPU-time budget:

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	MaxCPUTime: 250 * time.Millisecond,
})
```

`MaxCPUTime: 0` is the default and leaves the existing execution path
unchanged. The budget is checked at existing executor checkpoints. A query
that reaches the limit returns an error matching both
`hatSql.ErrSQLCPUTimeExceeded` and `context.Canceled`, so query managers can
classify it as cooperative cancellation. A function that does not return to an
executor checkpoint can exceed the budget until it yields back to the query
engine.

On Linux, the implementation reads `getrusage(RUSAGE_THREAD)` and sums CPU-time
deltas for every participating OS thread. Other platforms return
`ErrSQLCPUTimeUnsupported` when a nonzero CPU budget reaches its first check;
they do not silently treat wall time as CPU time.

The default sampling quantum is 64 checkpoints:

```go
SQLQueryOptions{
	MaxCPUTime:       250 * time.Millisecond,
	CPUTimeCheckEvery: 1, // strictest cooperative sampling; higher cost
}
```

`CPUTimeCheckEvery` is bounded by `MaxSQLCPUTimeCheckEvery`. Zero uses
`DefaultSQLCPUTimeCheckEvery`; one samples every checkpoint. With the default
quantum, cancellation can overshoot by up to 63 checkpoints plus the current
operator work.

## Benchmark

Commands:

```text
make benchmark-c233-baseline
make benchmark-c233-cpu-budget
```

Five 200 ms samples on an AMD Ryzen 9 5950X:

| Path | Samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Existing checkpoint before C233 | 4.414, 4.273, 4.496, 4.382, 4.537 | 4.414 ns | 0 | 0 |
| Existing checkpoint after C233 | 3.988, 3.916, 3.862, 3.902, 4.425 | 3.916 ns | 0 | 0 |
| CPU budget, default quantum 64 | 22.46, 20.45, 18.32, 19.58, 18.80 | 19.58 ns | 0 | 0 |
| CPU budget, every checkpoint | 821.8, 1042, 858.1, 891.2, 798.9 | 858.1 ns | 0 | 0 |

The default-off path has no CPU clock call or allocation. Opt-in sampling adds a
small checkpoint cost and bounds syscall frequency; strict sampling is
available when tighter cancellation latency is more important than throughput.
