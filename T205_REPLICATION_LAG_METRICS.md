# T205: LSN Replication Lag And Apply Throughput

`hatReplication.ReplicaLSNMetrics` provides bounded, caller-driven metrics for
replication consumers that expose a source log sequence number (LSN) and the
latest LSN durably applied by a replica.

```go
metrics, err := hatReplication.NewReplicaLSNMetrics(hatReplication.ReplicaLSNMetricsOptions{
	MaxSpaces: 256,
})
if err != nil {
	return err
}

status, err := metrics.Observe("orders", sourceLSN, appliedLSN, observedAt)
if err != nil {
	return err
}
fmt.Println(status.LagLSN, status.ApplyThroughputPerSecond)
```

## Semantics

- `SourceLSN` and `AppliedLSN` are monotone per space. A regressed LSN or
  timestamp is rejected and leaves the previous status unchanged.
- `LagLSN` is `max(SourceLSN-AppliedLSN, 0)`. This avoids negative lag when a
  sampled source watermark is temporarily older than a replica's applied
  watermark.
- Throughput is the LSN delta divided by elapsed observation time. The first
  observation and equal-timestamp observations report zero throughput.
- `Snapshot` returns an independent lexicographically ordered slice. Sorting is
  deliberately kept out of `Observe` so the hot update path stays allocation
  free after a space is registered.
- The tracker retains at most `MaxSpaces` names and states. The default is 256;
  the maximum is 65,536. A new space is rejected once the bound is reached.
- A failover that changes the LSN epoch should use a new space identity or a
  new tracker. The API does not silently reinterpret a reset as progress.

The tracker does not start a worker, inspect a WAL, or change replication
behavior. Existing replication remains unchanged until an application creates
one and feeds it observations.

## Safety

Space names are trimmed, bounded to 256 bytes, and NUL-free. The tracker stores
only the name, LSNs, timestamps, and derived rates; it does not retain row keys,
values, credentials, or payloads. Snapshot results are copies, and failed
observations do not partially update state.

## Benchmark

Linux `amd64`, AMD Ryzen 9 5950X, `go test -benchmem -count=5 -benchtime=200ms`.
The observation benchmark updates an already-registered space. The snapshot
benchmark copies and sorts 256 spaces.

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Observe, existing space | 54.18 | 0 | 0 |
| Snapshot, 256 spaces | 40,384 | 21,912 | 4 |

The initial implementation measured 72.25 ns/op, 8 B/op, and 1 allocation for
the existing-space observation path. Moving the space-name clone to first
registration reduced that path by 1.33x and removed its allocation. Snapshot
cost was effectively unchanged by that optimization.

Raw five-sample observations:

```text
Observe before clone fix: 73.94 72.25 72.42 70.68 71.81 ns/op, 8 B/op, 1 alloc/op
Observe after clone fix:  53.79 54.39 54.18 55.07 53.99 ns/op, 0 B/op, 0 alloc/op
Snapshot after clone fix: 40384 39608 41034 41112 39866 ns/op, 21912 B/op, 4 alloc/op
```

The complete benchmark output and command are recorded in
[BENCHMARK.md](BENCHMARK.md#t205-replication-lag-and-apply-throughput).

## Verification

The focused tests cover first observations, lag/rate derivation, deterministic
snapshots, copy isolation, invalid spaces, monotonicity, timestamp regression,
capacity bounds, zero-value behavior, and default timestamps. The full
replication package test suite also passes.
