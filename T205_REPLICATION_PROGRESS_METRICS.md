# T205 Replication Progress Metrics

`hatReplication.ReplicationProgressMetrics` is an opt-in, bounded collector
for replication lag and apply throughput. It is transport-neutral: the caller
feeds source and replica journal observations from its existing HTTP, gRPC, or
local applier path.

## Configuration

```go
metrics, err := hatReplication.NewReplicationProgressMetrics(
	 hatReplication.ReplicationProgressMetricsOptions{
		Enabled:    true,
		MaxTargets: 64,
	},
)
```

The zero-value option is disabled. A disabled collector allocates no target
map and returns `ErrReplicationProgressMetricsDisabled` from `Observe`. A
`MaxTargets` value of `0` selects the default of 64; values above the hard
bound of 4096 are rejected. This prevents an untrusted target label from
creating an unbounded metric map.

## Observing Progress

```go
err := metrics.Observe(hatReplication.ReplicationProgressObservation{
	TargetID:     "replica-eu-1",
	SourceLSN:    1200,
	AppliedLSN:   1188,
	AppliedBytes: 48_000_000,
	ObservedAt:   time.Now(),
})
```

`SourceLSN` and `AppliedLSN` are monotone journal positions. `AppliedBytes`
is optional and can remain zero when the applier does not expose byte counts.
The collector rejects an applied LSN beyond the observed source LSN, target
progress regressions, timestamp regressions, and non-identical progress at the
same timestamp. Exact duplicate observations are idempotent.

The source LSN is a global high-water mark. A delayed target may report an
older source LSN without lowering the global value; its lag is therefore
calculated against the newest source position observed by any target.

## Reading Metrics

```go
snapshot := metrics.Snapshot()
for _, target := range snapshot.Targets {
		fmt.Println(target.TargetID, target.LagLSN, target.ApplyLSNPerSecond)
}
```

`Snapshot` is detached and sorts targets by `TargetID`, so it is safe to
serialize or modify after the call. Each target includes:

- `LagLSN`: the saturated difference between the global source and applied LSN;
- `ApplyLSNPerSecond`: integer LSN progress over the previous target sample;
- `ApplyBytesPerSecond`: integer byte progress over the previous target sample;
- the latest applied LSN, byte total, and observation timestamp.

The first sample for a target reports zero rates. Rates use integer arithmetic
and saturate at `math.MaxUint64` instead of wrapping on very short intervals.
The collector does not infer operations per second from an LSN; callers should
export an operation rate separately when an LSN does not represent one record.

## Cost

On Linux/amd64 with an AMD Ryzen 9 5950X, five-sample median benchmarks were:

| Workload | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Direct lag/rate arithmetic control | 0.5175 | 0 | 0 |
| Existing-target `Observe` | 58.78 | 0 | 0 |
| One-target `Snapshot` | 186.0 | 80 | 1 |

`Observe` updates an existing target without allocation. `Snapshot` allocates a
detached target slice and sorts IDs, which is intentional for safe monitoring
serialization. The collector is not automatically attached to replication;
callers choose a sampling interval and whether the snapshot allocation belongs
on their monitoring path.

Run the measurements with:

```text
make benchmark-t205-baseline
make benchmark-t205
```
