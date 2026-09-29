# T206 Deterministic Replica Bootstrap And Join

The existing snapshot-plus-WAL join helper and TU09 lifecycle coordinator now
also support bounded restart checkpoints. This closes the operational gap
between a process restart and a deterministic continuation of the same join.

## API

```go
checkpoint, err := coordinator.MarshalSnapshot()
if err != nil {
    return err
}
if err := os.WriteFile(path, checkpoint, 0o600); err != nil {
    return err
}

restarted, err := hatReplication.LoadSnapshotWALBootstrapCoordinator(
    path,
    hatReplication.SnapshotWALBootstrapOptions{},
)
```

For local persistence, `coordinator.Save(path)` performs the complete atomic
write with a `0600` temporary file, file sync, rename, and parent-directory
sync. `LoadSnapshotWALBootstrapCoordinator` validates the bounded binary frame
before publishing any state. `RestoreSnapshot` refuses to overwrite a
coordinator that has already started a join.

The frame is deterministic binary data with a version, length, and CRC32C. It
contains only the normalized plan, lifecycle phase/generation, applied journal
sequence, and bounded abort reason. It never contains WAL records, auth
tokens, or filesystem paths. Corrupt, truncated, trailing, non-canonical, or
incompatible state is rejected without mutating the destination coordinator.

## Restart Workflow

1. Create a coordinator and `Begin` one exact snapshot/WAL plan.
2. Install the snapshot, then call `AdvanceWAL` only after contiguous replay.
3. Persist `Save(path)` after meaningful progress or at the operator's
   checkpoint interval.
4. On restart, call `LoadSnapshotWALBootstrapCoordinator` and continue with
   the returned generation and fencing token.
5. Call `MarkReady` and `Activate` only after the caller's integrity, topology,
   and traffic checks succeed.

The state machine remains transport-neutral. Snapshot transfer, WAL replay,
authentication, checksums for the actual snapshot, membership publication, and
traffic activation remain caller-owned. The persisted checkpoint prevents a
stale or partially reconstructed local lifecycle from being treated as a
ready replica; it does not replace source-side fencing or consensus.

## Measurement

Run `make benchmark-t206`. Existing transition reads remain effectively
unchanged and zero-allocation. Checkpoint encode/decode are explicit control
plane work; durable `Save` includes file and directory sync and is therefore
orders of magnitude slower than in-memory transitions.

See [BENCHMARK.md](BENCHMARK.md#t206-deterministic-replica-bootstrap-checkpoints)
for raw samples and the exact before/after tradeoff.
