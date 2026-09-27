# M033e Global Timestamp Lease Pool

`hatReplication.GlobalTimestampLeasePool` is the client-side range-lease layer
for the existing global timestamp oracle. It keeps the coordinator, consensus,
leader election, transport, and durable snapshot responsibilities unchanged,
while avoiding one coordinator reservation for every write.

```go
oracle, _ := hatReplication.NewGlobalTimestampOracle(1, 0)
pool, _ := hatReplication.NewGlobalTimestampLeasePool(
	 hatReplication.GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "region-a-1",
		NodeEpoch: 7,
		BatchSize: 64,
		Reserve:   oracle.Reserve,
	})
timestamp, err := pool.Next()
```

For a remote coordinator, set `Reserve` to a closure around
`hatCache.GlobalTimestampReserveGRPCClient.Reserve`. The pool retries the same
request sequence after an error, so an idempotent coordinator can return the
original grant after a lost response.

`Observe` fences unused local values when a higher remote timestamp arrives.
Those values are intentionally discarded rather than reused, preserving
monotone ordering. A process crash can likewise leave a bounded gap of at most
`BatchSize - 1` unused values. The default batch is 64 and the maximum is
`1 << 20`; callers can choose a smaller batch when gaps matter more than
coordinator traffic.

## Benchmark

Command:

```text
make benchmark-m033e-lease-pool
```

Five samples, local in-process coordinator callback, AMD Ryzen 9 5950X:

| Path | ns/op samples | Median ns/op | B/op | allocs/op | Reservations/op |
| --- | --- | ---: | ---: | ---: | ---: |
| Direct one-timestamp reservation | 50.83, 48.38, 51.36, 51.17, 51.26 | 51.17 | 0 | 0 | 1.00000 |
| Lease pool, batch 64 | 8.035, 8.173, 8.063, 8.049, 8.127 | 8.063 | 0 | 0 | 0.01563 |

The pool is approximately 6.3x faster for timestamp issuance in this local
baseline and reduces coordinator calls by 64x. The benchmark excludes network
latency, so the RPC reduction is the important production benefit. The cost is
bounded timestamp gaps when a lease is abandoned; the pool never silently
reuses those values.
