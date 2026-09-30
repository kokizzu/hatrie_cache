# M221 Compute Cluster Resource Budgets

Named SQL compute clusters already isolate worker and queue capacity. M221 adds
an optional independent resident-memory budget to each named cluster, using the
existing FIFO `SQLMemoryAdmission` controller.

```go
manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
    ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
        "analytics": {
            Workers:                   4,
            QueueCapacity:             32,
            MemoryBudgetBytes:         512 << 20,
            MemoryAdmissionMaxPending: 64,
        },
    },
})
defer manager.Close()

result, err := manager.Execute(ctx, query, resolver, parameters, hatSql.QueryOptions{
    ComputeCluster:        "analytics",
    MemoryReservationBytes: 64 << 20,
})
```

When `MemoryBudgetBytes` is positive, a routed query must provide a positive
`MemoryReservationBytes`. The cluster-owned admission queue is authoritative,
so a caller cannot bypass the cluster boundary by supplying another admission
controller. Requests wait FIFO up to `MemoryAdmissionMaxPending`; a zero value
uses the existing bounded default. Reservations are released when execution
returns, including cancellation and error paths.

`ComputeClusterMemoryStats("analytics")` exposes `MaxBytes`, `ActiveBytes`,
`Pending`, `Admitted`, `Waited`, `Rejected`, and `Canceled` for monitoring.
Unbudgeted clusters return zero-valued stats and retain the existing behavior.
The feature is opt-in; the default direct execution path and unbudgeted named
clusters are unchanged.

## Tradeoff

The budget controls declared peak working memory. It is deliberately not a
runtime heap sampler: callers must choose conservative reservations, while the
queue provides deterministic admission and bounded waiter memory. A budgeted
cluster therefore trades concurrency for a hard, observable memory boundary.
