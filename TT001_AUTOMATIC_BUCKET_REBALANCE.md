# TT-001 Automatic Bucket Rebalancing

Status: partially adopted.

`hatTopology.PlanAutomaticBucketRebalance` derives a deterministic target
primary assignment from the current sharded topology. It keeps virtual shard
IDs and bucket ranges stable, excludes maintenance nodes by default, supports
an optional primary-move limit, preserves each shard's replica count, and
returns the existing `BucketMigrationPlan` values for caller-owned copy and
cutover through `BucketMigrationCoordinator`.

Planning does not copy records, contact peers, or publish the target. This is
deliberate: transport, snapshot/WAL transfer, consensus, and final ownership
publication remain explicit operational steps.

## Defaults and Safety

```go
plan, err := hatTopology.PlanAutomaticBucketRebalance(current,
    hatTopology.AutomaticBucketRebalanceOptions{})
```

The zero value allows all primary moves and excludes nodes marked
`Maintenance`. Set `MaxPrimaryMoves` to bound one planning/cutover wave. Set
`IncludeMaintenance` only for controlled maintenance recovery. A no-op plan
does not advance the fencing token; a plan with primary changes advances it by
one. Invalid topology, no eligible primary nodes, fencing overflow, and an
unpreservable replica contract are rejected without modifying the input.

## Benchmark

Measured with `make benchmark-tt001-automatic-rebalance` on an AMD Ryzen 9
5950X, Go `amd64`, five benchmark samples per case. The workload used 4,096
buckets, 32 virtual shards, eight nodes, and two replicas per shard; all
initial primaries were on one node.

| Path | Time (5-run range) | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Existing `PlanBucketMigrations` with target already prepared | 2.519-2.693 ms | 3,295,986-3,296,086 | 21,994-21,995 |
| Automatic target generation plus migration planning | 2.169-2.294 ms | 3,350,463-3,350,612 | 22,399-22,400 |

The automatic path added about 1.65% resident planning bytes and 1.8%
allocations in this run. Its measured elapsed range was not slower, but the
two paths do different work, so the useful decision is that automatic
rebalancing has a small bounded planning cost and no default runtime cost.
Actual data movement remains outside this benchmark.

## Remaining Work

Automatic transfer execution, peer health feedback, consensus-backed target
publication, persistent resumable plans, and weighted load metrics are still
caller-owned. Those are separate concerns because silently adding network or
background activity would change the existing default behavior.
