# CH-033 Distributed SQL Query Fan-out

Status: implemented as an opt-in execution primitive in `hat/hatSql`.

This adopts the useful part of ClickHouse distributed-query execution without
introducing a new network protocol or making every SQL query remote. The caller
provides independent shard resolvers, and the coordinator runs the same read
query against them with bounded concurrency.

## API

```go
result, err := hatSql.ExecuteSQLDistributedQuery(
    ctx,
    "FROM CACHE('users') SELECT id, region",
    []hatSql.SQLDistributedQueryShard{
        {ID: "asia", Resolver: asiaResolver},
        {ID: "eu", Resolver: euResolver},
    },
    nil,
    hatSql.SQLQueryOptions{},
    hatSql.SQLDistributedQueryOptions{
        MaxConcurrency: 8,
        MaxRows:        10000,
    },
)
```

With a nil `Merge`, rows are concatenated in the caller's shard-list order.
Completion order is never exposed, so a slow shard cannot reorder results.
Column names must match and per-shard pagination is rejected because it cannot
be combined safely by concatenation.

Use `SQLDistributedQueryOptions.Merge` for a global `ORDER BY`, `GROUP BY`,
global `LIMIT`, deduplication, or another operation that needs cross-shard
state. The merge function receives all successful shard results in input order.
The coordinator applies `MaxRows` after the custom merge as a final safety
bound.

## Bounds and failure behavior

- Default concurrency is 8; the configured value is capped at 256.
- `MaxRows` is optional and can inherit `SQLQueryOptions.MaxRows`.
- Empty, duplicate, or blank shard IDs are rejected.
- A nil resolver is rejected before any query starts.
- A shard error cancels queued work and returns no partial result.
- Caller cancellation is returned as `context.Canceled` or `context.DeadlineExceeded`.
- One shard uses the existing single-query path, avoiding worker-pool overhead.
- This is an execution primitive, not automatic SQL topology discovery. The
  caller remains responsible for shard selection, authentication, retries, and
  global merge semantics.

## Measurement

The benchmark uses eight independent resolvers, one row per shard, a 1 ms
resolver delay, `GOMAXPROCS=8`, five samples, and `-benchmem` on Linux/amd64
with an AMD Ryzen 9 5950X. The sequential path is the pre-feature equivalent;
the bounded fan-out path is the new implementation.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative wall time |
| --- | ---: | ---: | ---: | ---: |
| Sequential baseline | 8,576,363 | 32,305 | 126 | 1.00x |
| Bounded fan-out | 1,180,500 | 36,954 | 151 | 7.27x faster |

The latency win is about 7.3x for this network-like workload. The coordinator
cost is +4,649 B/op and +25 allocations/op, or about 1.14x bytes and 1.20x
allocations. That overhead is why the feature is opt-in and why local, cheap
single-node queries should continue using `ExecuteSQLQueryParameters` directly.

Raw samples:

```text
sequential:
139 8596145 ns/op 32306 B/op 126 allocs/op
139 8564292 ns/op 32305 B/op 126 allocs/op
139 8577410 ns/op 32305 B/op 126 allocs/op
139 8576363 ns/op 32305 B/op 126 allocs/op
139 8569175 ns/op 32305 B/op 126 allocs/op

bounded fan-out:
969 1172970 ns/op 37025 B/op 151 allocs/op
984 1198147 ns/op 36954 B/op 151 allocs/op
1002 1180500 ns/op 36946 B/op 151 allocs/op
996 1164486 ns/op 36944 B/op 151 allocs/op
1003 1187365 ns/op 36963 B/op 151 allocs/op
```

Correctness coverage includes deterministic merge order, bounded concurrent
execution, custom merge and row limits, input validation, cancellation, full
package tests, and the race detector.
