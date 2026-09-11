# Managed Refresh Freshness

The managed refresh scheduler can report an optional freshness threshold for
each materialized view or rollup. This is a Materialize-style freshness SLA
signal for monitoring and admission decisions; it does not change refresh
scheduling or reject reads automatically.

```go
err := scheduler.AddRollupWithOptions("metrics", hatSql.ManagedRefreshTaskOptions{
    Every:        time.Minute,
    MaxStaleness: 5 * time.Minute,
}, refresh)
if err != nil {
    return err
}

status, ok := scheduler.Status("metrics")
if ok && status.Stale {
    alert("metrics refresh is stale")
}
```

Use `AddMaterializedViewWithOptions` for materialized views. The existing
`AddMaterializedView` and `AddRollup` methods remain unchanged and keep
`MaxStaleness` disabled. A negative threshold is rejected; zero disables stale
reporting.

`Status`, `StatusAt`, and `StatusesAt` expose the task interval, next run,
running state, last attempt, last successful refresh, and last error. A task
with a positive threshold is stale before its first successful refresh and when
the age of its last successful refresh reaches the threshold. A failed attempt
updates `LastError` but does not replace `LastSuccessAt`, so operators can see
both the current failure and the last known-good freshness point.

The status is in-memory and process-local. It is intended for metrics, health
checks, or external control-plane policy. It does not validate row contents or
make a refresh exactly once across processes.

## Measured Cost

`make benchmark-mz041-refresh-freshness` measures one successful task followed
by repeated `StatusesAt` calls:

| Operation | Median time | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| `StatusesAt` for one task | 129.4 ns | 144 | 1 |

This cost is paid only when a caller asks for status and does not appear on
normal SQL reads, writes, or refresh callbacks. Raw samples are in
[BENCHMARK.md](BENCHMARK.md).
