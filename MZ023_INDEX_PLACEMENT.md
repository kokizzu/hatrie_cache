# MZ-023 Index Placement

Hatrie SQL now has an opt-in contract for routing indexed equality and range
lookups to a named compute cluster. This follows Materialize's `IN CLUSTER`
idea without changing the default resolver API or requiring a cluster manager.

## Usage

Set `SQLQueryOptions.ComputeCluster` on the query:

```go
result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, args, hatSql.SQLQueryOptions{
	ComputeCluster: "analytics",
})
```

An equality resolver opts in by implementing:

```go
ResolveSQLIndexedSourceInCluster(name, key, field, cluster string, value interface{}) ([]hatSql.Row, bool, error)
```

For range indexes, implement
`ResolveSQLIndexedRangeSourceInCluster`. A named forced strategy can use the
corresponding `ClusterStrategyIndexedSourceResolver` or
`ClusterStrategyRangeIndexedSourceResolver` contract.

The resolver owns the placement map and returns `available=false` when the
requested cluster has no ready index. SQL then falls back to the source scan,
preserving correctness. A forced index still returns an error when its
requested placement is unavailable.

Legacy resolvers remain compatible. If they do not implement a cluster-aware
method, the existing resolver method is used, even when `ComputeCluster` is
set. This permits incremental adoption, but applications that require hard
placement isolation should implement the cluster-aware method and return false
for unavailable placements.

`SQLIndexDefinition.Cluster` and `SQLIndexHint.Cluster` are also available to
the planner helper `ExplainSQLIndexStrategy`. A candidate in another cluster
is reported as `cluster_mismatch`; a placed candidate selected without a
requested cluster is reported as `cluster_required`.

## Defaults and safety

- `ComputeCluster` is empty by default.
- Existing index resolver methods and query results are unchanged by default.
- A cluster miss never returns incomplete indexed results; it scans instead.
- The feature does not create, move, replicate, or rebalance indexes.
- Native dataflow paths that do not use the indexed source resolver remain
  unchanged; use a resolver-backed path when placement routing is required.

## Measurement

Measured on Linux/amd64, AMD Ryzen 9 5950X, with
`make benchmark-mz023-index-placement` and five benchmark samples:

| Path | Time | Memory | Allocs |
| --- | ---: | ---: | ---: |
| Legacy equality resolver | 31.1-31.4 ns/op | 8 B/op | 1 |
| Cluster-aware equality resolver | 33.0-33.5 ns/op | 8 B/op | 1 |
| Global strategy selection, 32 candidates | 2.66-2.82 us/op | 4,296 B/op | 4 |
| Cluster-filtered strategy selection, 32 candidates | 2.93-3.13 us/op | 4,296 B/op | 4 |

The cluster-aware equality dispatch adds about 2 ns per indexed lookup in this
microbenchmark and does not add allocations. Strategy filtering adds about
0.3 us for 32 candidates and does not add memory. The extra cost is opt-in;
the default unplaced strategy path keeps the fast path and the existing
allocation profile.
