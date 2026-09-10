# Common SQL Source Frontiers

`hatSql.SQLSourceFrontierTracker` is a Materialize-style consistency barrier
for a fixed set of independent source partitions. It records monotone
frontier observations and exposes the minimum observed frontier as the point
at which a cross-partition snapshot is safe to read.

## Example

```go
tracker, err := hatSql.NewSQLSourceFrontierTracker([]hatSql.SQLSourceFrontierPartition{
    {Source: "orders", Partition: "0"},
    {Source: "orders", Partition: "1"},
})
if err != nil {
    panic(err)
}
_, _ = tracker.Observe(hatSql.SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 42})
_, _ = tracker.Observe(hatSql.SQLSourceFrontier{Source: "orders", Partition: "1", Frontier: 45})
frontier, ready := tracker.CommonFrontier()
// frontier == 42, ready == true
```

`ReadyAt(42)` is true while `ReadyAt(43)` is false. An observed frontier of
zero is distinct from an unobserved partition. `ObserveBatch` validates all
partition names and duplicate updates before publishing any member of the
batch.

## Contract

- The configured partition set is fixed at construction time.
- Source and partition names are trimmed, must be non-empty, and must be
  unique as a pair.
- Equal or older observations are idempotent no-ops.
- `CommonFrontier`, `ReadyAt`, `Frontier`, and `Snapshot` are safe during
  concurrent observations.
- The heap root makes common-frontier reads O(1); a monotone update repairs the
  indexed heap without allocating.
- The tracker coordinates a caller's snapshot operation but does not itself
  pause or copy a `SourceResolver`. Pair it with `SQLSnapshotProvider` or a
  source-specific wait/lease mechanism when a physical read barrier is needed.

The tracker is opt-in and does not change ordinary SQL execution or source
offset tracking. It retains one small state entry per configured partition.

## Bounded Snapshot Barrier

`SQLSourceFrontierBarrier` adds an opt-in, context-aware wait over the tracker.
It wakes blocked callers when observations advance and returns only after every
configured source partition has observed at least the requested frontier. A
frontier of zero still waits for the initial observation from every partition.

```go
tracker, err := hatSql.NewSQLSourceFrontierTracker([]hatSql.SQLSourceFrontierPartition{
	{Source: "orders", Partition: "0"},
	{Source: "orders", Partition: "1"},
})
if err != nil {
	return err
}
barrier, err := hatSql.NewSQLSourceFrontierBarrier(tracker)
if err != nil {
	return err
}

if _, err := barrier.ObserveBatch([]hatSql.SQLSourceFrontier{
	{Source: "orders", Partition: "0", Frontier: 42},
	{Source: "orders", Partition: "1", Frontier: 42},
}); err != nil {
	return err
}
frontier, err := barrier.WaitForFrontier(ctx, 42)
if err != nil {
	return err
}
_ = frontier
```

Call `WaitForFrontier` before `SQLSnapshotProvider.BeginSQLSnapshot` when the
provider uses the same source frontier. The barrier does not copy or pause a
source resolver, elect a leader, or provide replication. Publish observations
through the barrier; direct mutation of the wrapped tracker cannot notify
blocked waiters. Invalid batches remain atomic.

### Ready-Path Benchmark

Run `make benchmark-m032c-frontier`. Five `-benchmem` samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X and 1,024 configured partitions.

| Path | Raw ns/op | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `SQLSourceFrontierTracker.ReadyAt`, before | 4.744; 4.632; 4.896; 4.813; 4.577 | 4.744 | 0 | 0 |
| Existing `SQLSourceFrontierTracker.ReadyAt`, final run | 4.945; 4.934; 4.928; 4.941; 4.970 | 4.941 | 0 | 0 |
| `SQLSourceFrontierBarrier.WaitForFrontier`, final | 2.858; 2.815; 2.837; 2.836; 2.906 | 2.837 | 0 | 0 |

The cached ready path is `1.74x` faster than the same-run existing readiness
check, with no measured allocations. The improvement comes from an atomic
cached common frontier; the blocking path pays only when a caller actually
has to wait.

## Benchmark

Command: `make benchmark-m032-frontier`.

The workload tracks 1,024 observed partitions, advances one partition, and
reads the common frontier. The baseline scans all partition values. Five
samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Scan all partitions before implementation | 481.8; 482.9; 479.1; 479.2; 488.3 | 481.8 | 0 | 0 |
| Scan all partitions final baseline | 487.8; 481.5; 483.8; 479.0; 479.8 | 481.5 | 0 | 0 |
| Indexed heap update and read | 181.9; 182.3; 179.4; 179.7; 181.5 | 181.5 | 0 | 0 |

The indexed path is about `2.65x` faster with the same measured transient
bytes and allocation count. The update path is O(log partitions); the common
frontier read is O(1).

## Verification

```text
make test-m032-frontier
make test-race-m032-frontier
make benchmark-m032-frontier
```
## Frontier-Bound Physical Snapshots

When a storage resolver can create an immutable view at an exact source
frontier, implement `SQLFrontierSnapshotProvider` and call
`BeginSQLFrontierSnapshot`. This composes the barrier's logical readiness with
the provider's physical atomicity contract and rejects providers that can only
create an unbounded snapshot. See [SQL_FRONTIER_SNAPSHOTS.md](SQL_FRONTIER_SNAPSHOTS.md)
for the API and benchmark.
