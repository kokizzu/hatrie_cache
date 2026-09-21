# MZ-047 Session Compute Routing

`hatSql` now supports opt-in named compute pools for managed query sessions.
Configure pools with `SQLQueryManagerOptions.ComputeClusters` and select one
per operation with `QueryOptions.ComputeCluster`:

```go
manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
	ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
		"analytics": {Workers: 2, QueueCapacity: 64},
		"interactive": {Workers: 4, QueueCapacity: 128},
	},
})
defer manager.Close()

result, err := manager.Execute(ctx, query, resolver, nil, hatSql.QueryOptions{
	ComputeCluster: "analytics",
})
```

An empty `ComputeCluster` keeps the existing manager default pool or direct
caller-goroutine execution. Unknown names fail synchronously, named pools
have bounded worker and queue settings, `ComputeClusters()` returns sorted
names for session discovery, and `Close` drains every configured pool.

This is process-local routing. It does not imply distributed scheduling,
replication, failover, or automatic workload classification; the caller still
chooses the session's cluster.

## Measurements

Five 500 ms samples on Linux/amd64, AMD Ryzen 9 5950X with `-cpu=1`. Both paths
use one worker and queue capacity eight and measure one managed query. The
named-pool path is compared with the existing unnamed managed pool.

| Path | Median time | Memory | Allocations | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing managed pool | 8,671 ns/op | 5,800 B/op | 30 | 1.00x |
| Named `analytics` pool | 8,399 ns/op | 5,800 B/op | 30 | 0.97x |

The result is effectively routing-neutral within scheduler noise: the named
selection adds no measured per-query allocations or retained bytes. Pool
creation and worker memory are opt-in costs, one per configured named pool.
