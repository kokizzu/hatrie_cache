# SQL Admission Control

`hatSql.NamespaceQueryGovernor` rejects or queues work before invoking the SQL
executor. This keeps expensive scans, joins, sorts, groups, and spill setup
behind explicit concurrency, queue, rate, and resource limits.

```go
governor, err := hatSql.NewNamespaceQueryGovernor(
    hatSql.NamespaceResourceLimits{
        MaxConcurrentQueries: 4,
        MaxQueuedQueries:      32,
        MaxQueriesPerWindow:   1000,
        MaxRows:               100_000,
        MaxJoinBytes:          64 << 20,
        MaxSortBytes:          64 << 20,
    },
    nil,
)
result, err := governor.Execute(ctx, "api", query, resolver, nil, options)
```

Admission proceeds in this order:

1. Validate the governor and namespace.
2. Acquire a namespace concurrency slot or wait in its bounded FIFO queue.
3. Observe context cancellation while waiting; canceled waiters are removed.
4. Consume the fixed-window rate quota, if enabled.
5. Tighten SQL row, join, result, worker, sort/group/set, spill, recursion,
   timeout, and related resource options.
6. Start the ordinary SQL executor.

A full queue returns `ErrNamespaceQueryQueueFull`. A depleted rate window
returns `ErrNamespaceQueryRateLimited`. A canceled context never starts the
query. The per-query execution controls continue to enforce row, byte, timeout,
and operator budgets after admission, so admission control is complementary to
scan-time limits rather than a replacement for them.

The gate and quota are process-local. They do not coordinate multiple server
processes or regions; use an external coordinator when admission must span
nodes. Policies only tighten caller options and namespace overrides cannot
escalate the configured defaults.

Focused coverage is in `hat/hatSql/governance_queue_test.go` and
`hat/hatSql/governance_quota_test.go`, including queue rejection, release,
cancellation-safe waiting, fixed-window rate limits, and configuration
validation.
