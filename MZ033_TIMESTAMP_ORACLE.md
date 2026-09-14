# MZ-033 Timestamp Oracle

## Status

MZ-033 is already implemented in the importable `hat/hatReplication` package.
This goal reconciles the catalog and benchmark documentation with that existing
implementation. No duplicate `hatPipeline` timestamp allocator is retained.

The package exposes two levels of coordination:

- `TimestampOracle` is a lock-free, process-local monotone Lamport-style clock.
- `GlobalTimestampOracle` is a transport-neutral serialized state machine for
  one consensus-selected or single-writer coordinator. It allocates contiguous
  ranges and fences retries and restarted node processes.

The global type does not elect a leader, replicate state, or make independent
processes share memory by itself. The caller must persist and replicate its
state through the deployment's control plane.

## Global Usage

```go
oracle, err := hatReplication.NewGlobalTimestampOracle(1, 0)
if err != nil {
    return err
}

grant, err := oracle.Reserve(hatReplication.GlobalTimestampRequest{
    Term:      1,
    NodeID:    "worker-a",
    NodeEpoch: 1,
    Sequence:  1,
    Observed:  0,
    Count:     1024,
})
if err != nil {
    return err
}

lease, err := grant.Lease()
if err != nil {
    return err
}
timestamp, ok := lease.Next()
```

`Reserve` observes the caller's timestamp and returns a globally unique
contiguous range. A repeated request with the same term, node, epoch, sequence,
observed value, and count returns the original grant without advancing the
clock. `NodeEpoch` must increase after a node restart, and `Sequence` must be
contiguous within an epoch. `AdvanceTerm` fences an older coordinator term.

Use a larger `Count` when a node can consume timestamps locally. `Lease.Next`
uses an atomic counter and does not allocate per timestamp. Unused values after
a node failure remain gaps; reusing them would risk publishing duplicate
timestamps.

## Snapshot And Recovery

`GlobalTimestampOracle.Snapshot` returns a deterministic
`GlobalTimestampOracleSnapshot`, sorted by `NodeID`. Store that value atomically
with the coordinator's consensus or durable state, then restore with
`NewGlobalTimestampOracleFromSnapshot`. The constructor validates node epochs,
request sequences, range boundaries, duplicate nodes, and overlapping grants.

Snapshots are transport-neutral Go values with JSON tags, so callers may use
their existing authenticated and checksummed replication format. The oracle
does not claim to provide durability or consensus on its own.

## Process-Local Clock

For a single process, use `NewTimestampOracle(initial)`. `Next` allocates the
next timestamp with an atomic compare-and-swap, `Current` reads the clock, and
`Observe` advances it when an incoming timestamp is newer. This type does not
provide cross-process uniqueness or durable recovery.

## Correctness Coverage

Existing `hat/hatReplication` tests cover ordered and non-overlapping ranges,
observed-clock jumps, idempotent retries, request conflicts, sequence gaps,
stale node epochs, term changes, snapshot round trips and validation, lease
exhaustion/reset, overflow, and concurrent node reservations.

Run the focused checks with:

```text
make test-mz033-timestamp-oracle
make race-mz033-timestamp-oracle
make vet-mz033-timestamp-oracle
make benchmark-mz033-timestamp-oracle
```

## Benchmark

The benchmark is an in-process microbenchmark on the current workspace. It
measures the coordinator's serialized reservation path and the local atomic
lease path; it is not a network or consensus benchmark. Each measurement below
is the median of five `-count=5` samples from the repository target, with
`-benchmem` enabled. The raw five samples are kept in `BENCHMARK.md`.

| Operation | Median | Allocations | Interpretation |
| --- | ---: | ---: | --- |
| `GlobalTimestampOracle.reserve_one` | 54.73 ns/op | 0 B/op, 0 allocs/op | One range reservation under the coordinator mutex |
| `GlobalTimestampOracle.reserve_range` (`Count=1024`) | 57.95 ns/op | 0 B/op, 0 allocs/op | Grant cost is effectively independent of range length |
| `GlobalTimestampOracle.leased_next` | 2.294 ns/op | 0 B/op, 0 allocs/op | Local atomic consumption, replenishing every 1,024 values |
| `TimestampOracle.Next` | 2.109 ns/op | 0 B/op, 0 allocs/op | Process-local atomic allocation |

The comparison is intentionally limited to the existing implementation. The
discarded duplicate draft's numbers must not be treated as product results.
