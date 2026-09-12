# Replica RPO Status

`hat/hatReplication` provides a small, clock-independent status value for
replicas. It turns a source sequence and each replica's applied sequence into
an observable replication lag and an RPO budget result.

## Why sequence lag

Wall-clock lag is hard to compare safely across machines because it depends on
clock synchronization and timestamp policy. A monotonically increasing source
sequence is deterministic: a replica is behind by the number of source
operations it has not applied yet.

## Single replica

```go
status := hatReplication.BuildReplicaRPOStatus("west-1", 100, 97, 4)
```

The result is:

```text
Node:               west-1
SourceSequence:     100
AppliedSequence:    97
LagSequences:       3
MaxRPOLagSequences: 4
RPOWithinBudget:    true
```

`maxLag == 0` means no sequence-lag budget is enforced. Applied sequences that
are ahead of the source are treated as zero lag rather than underflowing.

## Batch status

```go
statuses, err := hatReplication.BuildReplicaRPOStatuses(
    100,
    []hatReplication.ReplicaRPOInput{
        {Node: "west-1", AppliedSequence: 97},
        {Node: "east-1", AppliedSequence: 91},
    },
    4,
)
```

The output preserves input order:

```text
west-1: lag=3, within_budget=true
east-1: lag=9, within_budget=false
```

The batch builder rejects more than `MaxReplicaRPOStatuses` (4,096) inputs.
This bounds one caller-controlled allocation and keeps the helper suitable for
health endpoints, failover checks, and monitoring snapshots. It does not read
or mutate replication state; callers provide the source and applied sequence
values from their chosen replication metrics.

## Benchmark

Command:

```text
make benchmark-t-u07
```

Five runs of a 64-replica batch on the local AMD Ryzen 9 5950X environment:

| Run | ns/op | B/op | allocs/op |
| ---: | ---: | ---: | ---: |
| 1 | 1223 | 4096 | 1 |
| 2 | 1216 | 4096 | 1 |
| 3 | 1208 | 4096 | 1 |
| 4 | 1187 | 4096 | 1 |
| 5 | 1197 | 4096 | 1 |

Median: **1,208 ns/op**, **4,096 B/op**, **1 alloc/op**.

The implementation is deliberately allocation-bounded: it performs one result
slice allocation for the batch and no map, sorting, or clock work. Integration
with live replication counters and failover policy remains caller-owned.
