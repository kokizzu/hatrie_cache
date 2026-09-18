# CH-045 GLOBAL IN / GLOBAL JOIN Broadcast Planning

`hatSql.GlobalJoinBroadcastPlanner` is an importable planning primitive for
ClickHouse-style `GLOBAL IN` and `GLOBAL JOIN` execution. It identifies a
small, snapshot-consistent subquery result that can be materialized once and
reused by all workers instead of executing the same remote subquery once per
worker.

The planner stores only bounded fingerprint, epoch, and shape metadata. It does
not retain rows, perform network I/O, or change SQL execution automatically.
The caller owns result materialization, transport, publication, and payload
invalidation.

## Example

```go
planner, err := hatSql.NewGlobalJoinBroadcastPlanner(
	hatSql.GlobalJoinBroadcastPlannerOptions{Workers: 8},
)
if err != nil {
	return err
}

plan, err := planner.Plan(hatSql.GlobalJoinBroadcastRequest{
	Fingerprint: "global:customer-dimension:v3",
	Epoch:       42,
	Rows:        1024,
	Bytes:       1 << 20,
})
if err != nil {
	return err
}
// First eligible plan: one remote subquery, eight worker copies.
// A later call with the same fingerprint, epoch, rows, and bytes is a cache hit
// and reports zero remote subquery executions.
_ = plan
```

`Epoch` must change whenever the source snapshot can change. A fingerprint is
not a security credential; callers must authenticate the query and protect the
transport separately.

## Behavior And Bounds

- Results at or below both `MaxBroadcastRows` and `MaxBroadcastBytes` use
  `GlobalJoinBroadcastModeBroadcast`.
- The first eligible shape reports `RemoteSubqueryExecutions == 1`,
  `WorkerCopies == Workers`, and `BroadcastBytes == Bytes * Workers`.
- An identical fingerprint/epoch/shape reports `CacheHit == true` and zero
  remote subquery executions.
- Results above either threshold use
  `GlobalJoinBroadcastModePerWorker`, report one remote execution per worker,
  and are not cached.
- The metadata cache is FIFO-bounded by `MaxCachedPlans`; `Invalidate` removes
  all epochs and shapes for one fingerprint.
- Zero options use 65,536 rows, 64 MiB, and 4,096 cached plans. `Workers` is
  required and must be positive.
- Negative request sizes, missing fingerprints, invalid options, and fanout
  integer overflow are rejected before cache mutation.

## Measured Tradeoff

The benchmark uses eight workers and a 1 MiB eligible result. The naive control
only counts eight per-worker remote subquery executions; it does not model
network or database work. Five 500 ms samples were run with `-benchmem` on
Linux/amd64, AMD Ryzen 9 5950X.

| Path | Median time | Remote work/accounting | Memory |
| --- | ---: | --- | ---: |
| Naive per-worker accounting | 2.681 ns/op | 8 remote fetches/op | 0 B, 0 allocs |
| Cached broadcast plan hit | 39.10 ns/op | 0 remote fetches/op, 8 MiB fanout accounting | 0 B, 0 allocs |
| Cold broadcast plan | 117.0 ns/op | 1 remote fetch/op | 0 B, 0 allocs |

The planner adds a small local mutex/map cost, but its intended saving is
remote subquery execution and repeated remote database work, not arithmetic
throughput. It is opt-in and metadata-only, so existing distributed execution
is unchanged until a caller consumes the returned plan.

### Raw Five-Sample Results

```text
BenchmarkCH045NaivePerWorkerSubqueryAccounting: 2.906, 3.016, 2.681, 2.656, 2.671 ns/op; 8 remote-fetches/op; 0 B/op; 0 allocs/op
BenchmarkCH045GlobalJoinBroadcastPlanHit: 39.10, 44.92, 39.67, 38.89, 39.01 ns/op; 1 cache-hit/op; 8388608 fanout-bytes/op; 0 B/op; 0 allocs/op
BenchmarkCH045GlobalJoinBroadcastPlanCold: 114.8, 119.5, 116.4, 117.0, 118.9 ns/op; 1 remote-fetch/op; 0 B/op; 0 allocs/op
```

## Verification

```text
make test-ch045-global-broadcast
make benchmark-ch045-global-broadcast
```
