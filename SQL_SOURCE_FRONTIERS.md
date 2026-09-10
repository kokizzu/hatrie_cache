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
