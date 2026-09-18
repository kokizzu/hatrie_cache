# M032 Distributed Frontier Coordinator

`hatSql.SQLDistributedFrontierCoordinator` combines several independently
managed `SQLSourceFrontierBarrier` values into one common frontier. It is a
Materialize-inspired control-plane primitive for callers that partition source
coordination by region, database, tenant, or another ownership boundary.

The coordinator is opt-in and default-off. Existing SQL execution, source
resolution, snapshots, replication, transport, and leader election do not use
it automatically. The caller owns the mapping from source progress messages to
each barrier and decides when a common frontier is safe for a query or
snapshot.

## API

```go
barriers := []*hatSql.SQLSourceFrontierBarrier{regionEast, regionWest}
coordinator, err := hatSql.NewSQLDistributedFrontierCoordinator(barriers)
if err != nil {
	return err
}

frontier, ready := coordinator.CommonFrontier()
if !ready {
	return errFrontierNotReady
}
if !coordinator.ReadyAt(frontier) {
	return errFrontierNotReady
}

view, release, err := hatSql.BeginSQLDistributedFrontierSnapshot(
	ctx, provider, coordinator, frontier,
)
if err != nil {
	return err
}
defer release()
_ = view
```

`NewSQLDistributedFrontierCoordinator` rejects an empty list or nil barrier and
copies the input slice. `CommonFrontier` returns the minimum frontier across
all barrier groups. Its `ready` result is true only when every configured
partition in every group has produced an observation; groups may temporarily
have different frontier values. `ReadyAt` requires every group to have reached
the requested frontier.

`WaitForFrontier` waits on each barrier with the caller's context. The snapshot
helper waits for the requested common frontier and then calls the explicit
`SQLFrontierSnapshotProvider.BeginSQLSnapshotAt` contract. A provider that
cannot bind an exact frontier is rejected rather than silently opening an
unbounded snapshot.

## Ownership boundary

This is coordination, not distributed consensus. The caller must ensure that:

- each source partition publishes monotone observations to exactly one barrier;
- all barriers represent the same logical timestamp or frontier domain;
- source failures and membership changes are handled by the caller; and
- the provider can actually materialize an immutable view at the requested
  frontier.

The coordinator does not discover peers, replicate observations, elect a
leader, or persist source progress. Those concerns remain outside `hatSql` so
regional or multi-datacenter partitioning can choose its own durability and
failure policy.

## Cost

The steady-state coordinator check is O(number of barrier groups), with no
per-call allocation. Construction makes one defensive copy of the barrier
pointer slice, so its retained slice is approximately 8 bytes per barrier on a
64-bit system, in addition to normal slice/object overhead. Waiting is
sequential across groups but remains context-cancelable.

On Linux/amd64 with an AMD Ryzen 9 5950X, Go benchmark workers `-32`, and five
`-benchmem` samples:

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | --- |
| Manual loop over 8 barriers | 54.22; 53.08; 55.48; 57.11; 55.60 | 55.48 | 0 | 0 | baseline |
| Coordinator `ReadyAt` over 8 barriers | 56.56; 56.62; 55.82; 54.40; 58.48 | 56.56 | 0 | 0 | 1.02x cost, 1.9% slower |
| Coordinator construction over 8 barriers | 66.16; 68.48; 66.25; 64.45; 60.50 | 66.16 | 64 | 1 | setup-only |

The measured runtime cost is small and isolated to callers that opt in. This
feature is a consistency capability, not a claim that the wrapper is faster
than an equivalent hand-written loop. Reproduce with:

```text
make benchmark-m032-distributed-frontier
```

The focused correctness, race, and vet commands are:

```text
make test-m032-distributed-frontier
make race-m032-distributed-frontier
make vet-m032-distributed-frontier
```
