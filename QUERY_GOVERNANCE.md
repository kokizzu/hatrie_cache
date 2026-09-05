# Query Governance

`NamespaceResourceLimits.MaxQueuedQueries` bounds the number of queries that
may wait behind `MaxConcurrentQueries` for one namespace. A value of `0`
preserves the existing unlimited-waiter behavior. A positive value rejects a
new waiter with `hatSql.ErrNamespaceQueryQueueFull` before allocating a waiter
or entering the SQL executor.

Per-namespace limits only tighten the defaults. This makes a global safety
policy difficult to loosen accidentally through a tenant-specific overlay.
The queue limit is independent for each namespace and remains inactive when
`MaxConcurrentQueries` is zero.

Example:

```go
governor, err := hatSql.NewNamespaceQueryGovernor(
    hatSql.NamespaceResourceLimits{
        MaxConcurrentQueries: 8,
        MaxQueuedQueries:     64,
    },
    map[string]hatSql.NamespaceResourceLimits{
        "interactive": {MaxConcurrentQueries: 2, MaxQueuedQueries: 8},
    },
)
```

The uncontended gate fast path remains allocation-free. The focused benchmark
measures about `7.5-8.2 ns/op`, `0 B/op`, and `0 allocs/op` on the benchmark
host. Use `make test-query-governor`, `make race-query-governor`, and
`make benchmark-query-governor` to verify the behavior.
