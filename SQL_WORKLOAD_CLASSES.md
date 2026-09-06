# SQL Workload Classes

`hatSql.NamespaceQueryGovernor` provides static workload classes keyed by
namespace. Each class can tighten execution and memory limits without changing
the global SQL defaults.

```go
governor, err := hatSql.NewNamespaceQueryGovernor(
    hatSql.NamespaceResourceLimits{
        MaxConcurrentQueries: 8,
        MaxRows:              100_000,
        MaxGroupBytes:        64 << 20,
        MaxSortBytes:         64 << 20,
        MaxWorkers:           4,
    },
    map[string]hatSql.NamespaceResourceLimits{
        "reporting": {MaxConcurrentQueries: 2, MaxRows: 10_000},
    },
)
result, err := governor.Execute(ctx, "reporting", query, resolver, params, options)
```

The policy covers maximum concurrent and queued queries, fixed-window query
rate, rows, join work and bytes, result bytes, workers, sort/group/set bytes,
spill bytes, recursion depth, timeout, and spill-directory routing. A zero
field leaves that limit unset. An enabled rate quota with no window uses one
minute.

Namespace overrides are tightened against defaults. A namespace cannot use a
configuration overlay to increase a limit, extend a timeout, or escape a
default spill directory. The caller's stricter per-query options remain in
force. Negative values and invalid policy names are rejected during
construction.

The governor is an execution boundary, not a scheduler for arbitrary work. It
waits fairly behind the configured concurrency gate, honors context
cancellation for queued queries, rejects a full queue, and applies the final
resource ceilings before delegating to the normal SQL executor. The quota and
gate state are process-local and reset when the governor is recreated.

Focused coverage is in `hat/hatSql/governance_queue_test.go` and
`hat/hatSql/governance_quota_test.go`, including queue bounds, cancellation
behavior, limit tightening, fixed-window quotas, and invalid configuration.
