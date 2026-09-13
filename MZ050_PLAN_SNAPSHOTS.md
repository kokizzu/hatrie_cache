# MZ-050 SQL Plan Snapshots

`hatSql` now supports an opt-in immutable plan snapshot on materialized query
results. It records the query identity, query-start timestamp, the requested
source-frontier contract, and a deep copy of the explain steps. This follows
Materialize's emphasis on making temporal readiness explicit without changing
ordinary query semantics.

## Usage

Set `PlanSnapshot` on `SQLQueryOptions`:

```go
frontier := uint64(42)
result, err := hatSql.ExecuteSQLQueryContext(ctx, source, resolver, hatSql.SQLQueryOptions{
	QueryID:                "orders-report",
	RequireSourceFrontier:  true,
	RequiredSourceFrontier: frontier,
	PlanSnapshot:           &hatSql.SQLPlanSnapshotOptions{},
})
if err != nil {
	return err
}
fmt.Println(result.PlanSnapshot.Format)
fmt.Println(result.PlanSnapshot.RequiredSourceFrontier)
```

`SQLPlanSnapshotFormat` is `hatrie-cache-sql-plan-snapshot/v1`. The snapshot
contains:

| Field | Meaning |
| --- | --- |
| `format` | Stable wire-format identifier. |
| `query_id` | Query ID supplied by the caller or generated for snapshot capture. |
| `started_at_unix_nano` | Wall-clock diagnostic timestamp for query start. |
| `required_source_frontier` | Set only when `RequireSourceFrontier` is enabled. |
| `as_of_frontier` | Exact historical frontier when `AsOfFrontier` is set, including zero. |
| `steps` | Independently owned explain steps. |

The pointer is deliberately the enable switch. With the default `nil` value,
no snapshot is retained and the JSON result remains unchanged. Offset and
keyset materialized pages support the same option. Row-callback streaming
continues to expose its existing callback and does not allocate a materialized
snapshot.

Snapshots are separated in SQL result-cache keys from ordinary results, and
all snapshot pointers and nested plan slices are cloned before crossing a
cache or materialized-view boundary.

## Measured Tradeoff

The benchmark used `EXPLAIN SELECT id FROM CACHE('users')`, Go benchmark
workers `-32`, Linux `amd64`, an AMD Ryzen 9 5950X, and five samples per case.
The default case is the pre-feature execution contract; the enabled case pays
only when a caller requests the snapshot.

| Variant | Median ns/op | B/op | Allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default, snapshot disabled | 3,302 | 5,360 | 22 | 1.00x | 1.00x |
| Snapshot enabled | 3,604 | 5,954 | 28 | 1.09x | 1.11x |

The opt-in diagnostic adds `302 ns/op`, `594 B/op`, and `6 allocs/op` in this
small explain-plan fixture. This is bounded metadata-copy cost, not a query
execution optimization; it is kept off by default.

Raw command:

```text
make benchmark-mz050
```

Raw samples:

```text
BenchmarkMZ050PlanSnapshotDefault-32  360632  3102 ns/op  5360 B/op  22 allocs/op
BenchmarkMZ050PlanSnapshotDefault-32  369709  3165 ns/op  5360 B/op  22 allocs/op
BenchmarkMZ050PlanSnapshotDefault-32  308072  3302 ns/op  5360 B/op  22 allocs/op
BenchmarkMZ050PlanSnapshotDefault-32  342488  3341 ns/op  5360 B/op  22 allocs/op
BenchmarkMZ050PlanSnapshotDefault-32  346581  3369 ns/op  5360 B/op  22 allocs/op
BenchmarkMZ050PlanSnapshotEnabled-32  305214  3702 ns/op  5954 B/op  28 allocs/op
BenchmarkMZ050PlanSnapshotEnabled-32  317168  3824 ns/op  5954 B/op  28 allocs/op
BenchmarkMZ050PlanSnapshotEnabled-32  304014  3604 ns/op  5954 B/op  28 allocs/op
BenchmarkMZ050PlanSnapshotEnabled-32  334090  3593 ns/op  5954 B/op  28 allocs/op
BenchmarkMZ050PlanSnapshotEnabled-32  313767  3445 ns/op  5954 B/op  28 allocs/op
```
