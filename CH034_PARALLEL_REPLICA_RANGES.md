# CH-034 Parallel Replica Range Reads

Status: implemented as an opt-in transport-neutral coordinator in
`hat/hatReplication`.

This adopts the useful part of ClickHouse parallel replicas: split a read into
independent ranges, assign each range to one replica, execute ranges in
parallel, and merge the successful values in the original range order. It does
not add SQL planner topology discovery or duplicate a whole query on every
replica.

## API

```go
result, err := hatReplication.ExecuteParallelReplicaRangeRead(
    ctx,
    []string{"replica-a", "replica-b", "replica-c"},
    []hatReplication.ParallelReplicaRange{
        {ID: "part-0", Payload: part0},
        {ID: "part-1", Payload: part1},
    },
    hatReplication.ParallelReplicaRangeReadOptions{
        MaxConcurrency: 8,
    },
    func(ctx context.Context, node string, item hatReplication.ParallelReplicaRange) (any, error) {
        return readRange(ctx, node, item.Payload)
    },
)
```

Each range starts on a deterministic round-robin replica. A failed range is
retried on the next replicas, at most once per replica. A successful range is
not duplicated. Results are returned in input range order regardless of
completion order, which lets a caller concatenate or merge ordered fragments.

`MaxConcurrency` defaults to 8 and is bounded at 256. `RecordAttempts` is
disabled by default to keep the hot path small; enabling it retains per-range
replica start/completion/error details for diagnostics. Any exhausted range
cancels outstanding work and returns no partial result. Caller cancellation is
returned unchanged.

The caller still owns range discovery, replica health, authentication, SQL
predicate pushdown, and the final merge semantics. The existing
`ExecuteParallelReplicaRead` hedged-read API remains unchanged for the
single-query first-success use case.

## Measurement

The benchmark uses 32 ranges, a 1 ms read delay per range, four replicas,
`MaxConcurrency=8`, `GOMAXPROCS=8`, five samples, and `-benchmem` on Linux/amd64
with an AMD Ryzen 9 5950X. The baseline reads every range serially from one
replica; the candidate uses the new bounded coordinator with default
`RecordAttempts=false`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative wall time |
| --- | ---: | ---: | ---: | ---: |
| Serial one-replica baseline | 33,901,239 | 2,690 | 1 | 1.00x |
| Bounded parallel ranges | 4,297,946 | 8,218 | 30 | 7.89x faster |

The latency win is substantial for latency-bound independent ranges. The
coordinator adds 5,528 B/op and 29 allocations/op, or about 3.05x bytes and
30x allocations. This is why it remains opt-in and should not replace a cheap
local sequential scan. `RecordAttempts=true` adds diagnostic allocations and
should be reserved for troubleshooting or telemetry captures.

Raw samples:

```text
serial:
34 33905059 ns/op 2691 B/op 1 allocs/op
34 33904622 ns/op 2691 B/op 1 allocs/op
34 33894006 ns/op 2690 B/op 1 allocs/op
34 33885145 ns/op 2690 B/op 1 allocs/op
34 33901239 ns/op 2690 B/op 1 allocs/op

bounded:
277 4299638 ns/op 8269 B/op 30 allocs/op
279 4296399 ns/op 8199 B/op 30 allocs/op
278 4289680 ns/op 8223 B/op 30 allocs/op
279 4297946 ns/op 8218 B/op 30 allocs/op
277 4313835 ns/op 8204 B/op 30 allocs/op
```

Correctness coverage includes range-order stability, deterministic replica
balancing, bounded concurrency, failed-range retry, no-partial-result
failure, input validation, formatting, and the race detector.
