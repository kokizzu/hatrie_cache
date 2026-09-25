# MZ-021 Replica Hot Handoff

Materialize-inspired replica handoff for warming a replacement query replica
before route cutover. The feature is opt-in and transport-neutral: it moves a
caller-produced query-state snapshot, applies ordered deltas, verifies target
readiness, and returns a generation/fencing-bound promotion token.

## Why

A cold replacement can rebuild all query state before it serves traffic. A hot
handoff installs a consistent snapshot at frontier `N`, replays only the tail
after `N`, and lets the caller publish the replacement at the observed frontier.
The old source is not fenced automatically because the embedding service owns
its routing and cluster membership policy.

## Example

```go
handoff, err := hatSql.NewReplicaHotHandoff(hatSql.ReplicaHotHandoffOptions{
	SourceID:             "primary",
	TargetID:             "replica-b",
	ExpectedSchemaVersion: 7,
	BatchSize:            256,
	FencingToken:         currentFence,
})
if err != nil {
	return err
}
if err := handoff.Prepare(ctx, source, target); err != nil {
	return err
}
if err := handoff.CatchUp(ctx, source, target); err != nil {
	return err
}
state := handoff.State()
token, err := handoff.Promote(state.Generation, currentFence)
if err != nil {
	return err
}

// The caller must atomically publish token.TargetID at token.Frontier and
// fence the previous source before routing new reads.
_ = token
```

`source` implements `ReplicaHotHandoffSource`:

- `Snapshot` returns a consistent image with source ID, epoch, schema version,
  snapshot ID, and frontier.
- `Fetch` returns a bounded page after the supplied frontier and the source's
  observed current frontier.

`target` implements `ReplicaHotHandoffTarget`:

- `InstallSnapshot` atomically installs the image.
- `Apply` atomically applies an ordered delta page.
- `Ready` validates that the warmed state can serve reads.

The snapshot payload remains valid for the synchronous `InstallSnapshot` call.
If the target retains it after that callback, the target owns the required copy.
This avoids a redundant coordinator copy on large state images.

## Lifecycle and safety

1. `Prepare` validates source identity, epoch, schema, and image size, then
   installs the snapshot.
2. `SyncOnce` or blocking `CatchUp` rejects frontier regressions, epoch
   changes, sequence gaps, oversized pages, and empty pages that hide missing
   updates.
3. When the applied frontier equals the observed source frontier, `Ready` is
   called and promotion becomes possible.
4. `Promote` checks the state generation, fencing token, zero lag, and target
   readiness before returning the cutover token.

The coordinator starts no goroutines. It does not route queries, fence a
source, persist state, retry a failed transport, or discover cluster topology.
Those responsibilities remain explicit so a service can compose its existing
consensus, storage, and transport rules.

## Defaults and tradeoff

The default page size is 256 deltas, polling is 100 ms, and the maximum image
is 64 MiB unless the caller chooses smaller bounds. The default application
path is unchanged because no handoff is created automatically.

The measured coordinator cost includes a 64 KiB snapshot transfer, validation,
one tail delta, readiness, and lifecycle accounting. Removing a redundant
snapshot copy cut the median from 20.444 us / 131,648 B / 5 allocations to
9.024 us / 66,112 B / 4 allocations. See
[`BENCHMARK.md`](BENCHMARK.md#mz-021-replica-hot-handoff).
