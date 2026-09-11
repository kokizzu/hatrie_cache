# SQL Namespace Compute Pools

`NamespaceQueryGovernor` now supports an opt-in compute pool per named
namespace. This adopts the useful part of cluster and workload isolation found
in Materialize, ClickHouse, and Tarantool: a busy workload can have an
independent CPU admission budget instead of consuming every caller's
goroutine. The implementation is local to one process. It is not a cluster
membership, replication, or distributed scheduler.

## Configuration

`NamespaceResourceLimits` adds two fields:

```go
type NamespaceResourceLimits struct {
    ComputeWorkers       int
    ComputeQueueCapacity int
}
```

The default is deliberately off:

```go
governor, err := hatSql.NewNamespaceQueryGovernor(
    hatSql.NamespaceResourceLimits{},
    map[string]hatSql.NamespaceResourceLimits{
        "region-us": {ComputeWorkers: 2, ComputeQueueCapacity: 32},
        "region-eu": {ComputeWorkers: 2, ComputeQueueCapacity: 32},
    },
)
if err != nil {
    return err
}
defer governor.Close()

result, err := governor.Execute(
    ctx,
    "region-us",
    "SELECT * FROM orders WHERE customer_id = ?",
    resolver,
    []interface{}{customerID},
    hatSql.SQLQueryOptions{},
)
```

`ComputeWorkers: 0` preserves the existing caller-goroutine execution path.
When workers are enabled, a zero `ComputeQueueCapacity` selects the SQL
compute-pool default of `64`. Namespace overrides are tightened against the
governor defaults; zero in an override means that the default is inherited,
not that an enabled default pool is disabled. The public maximums are
`MaxSQLQueryManagerComputeWorkers` (`256`) and
`MaxSQLQueryManagerComputeQueueCapacity` (`100000`).

Namespaces with their own positive worker count receive independent pools.
Namespaces without one use the default pool when the default has workers, or
the legacy caller path when it does not. Named pools do not share queued work
with each other, so a saturated region cannot consume another region's queue.
The existing `MaxConcurrentQueries`, `MaxQueuedQueries`, query byte limits,
and work limits remain applicable and can be used together with the compute
pool. Per-query `SQLQueryOptions.Workers` should also be sized deliberately,
because it can create additional operator parallelism inside an admitted
query.

`Close` rejects new executions and drains work already admitted to the owned
pools. It does not cancel a running query; the query context remains the
cancellation boundary. There is no process-wide RSS limiter and no online
worker resize in this slice. Use a bounded number of operational classes such
as region, priority, or tenant tier rather than creating a pool for every
short-lived tenant.

This change does not alter SQL results, persistence bytes, wire formats,
backup/restore formats, or cluster join behavior.

## Tradeoff And Benchmark

The measured workload is a deterministic one-row SQL query executed through a
static resolver. Five samples were collected with `go test -cpu 32 -benchmem
-count=5` on `linux/amd64` using an AMD Ryzen 9 5950X. The baseline was
captured before the namespace-pool code was added; the legacy-after row checks
that the default path did not regress materially.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative to baseline |
| --- | ---: | ---: | ---: | --- |
| Legacy governor, before | 1,285 | 3,240 | 18 | `1.00x` |
| Legacy governor, after | 1,298 | 3,240 | 18 | `1.01x` time, same memory |
| Two named namespace pools | 3,190 | 4,032 | 23 | `2.48x` time, `1.24x` memory, +5 allocations |

The named-pool path is slower for tiny queries because it submits work,
coordinates a bounded queue, and transfers the result back to the caller. It
is therefore an isolation and admission-control feature, not a throughput
optimization. The default remains off so existing users pay none of that
per-query pool overhead. The small gate and disabled-quota controls remained
allocation-free at medians of `8.198 ns/op` and `3.275 ns/op` respectively.

### Raw Samples

| Benchmark | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | --- | --- |
| Legacy governor, before | 1,330; 1,316; 1,285; 1,275; 1,260 | 3,240; 3,240; 3,240; 3,240; 3,240 | 18; 18; 18; 18; 18 |
| Legacy governor, after | 1,427; 1,321; 1,297; 1,298; 1,272 | 3,240; 3,240; 3,240; 3,240; 3,240 | 18; 18; 18; 18; 18 |
| Named namespace pools | 3,138; 3,179; 3,263; 3,266; 3,190 | 4,032; 4,032; 4,032; 4,032; 4,032 | 23; 23; 23; 23; 23 |

Run the reproducible benchmark with:

```sh
make benchmark-mz019-resource-pools
```

Raw output is written to
`build/benchmarks/mz019-resource-pools.txt`.
