# T047i Coordinator-Owned Durable Write State

This is an opt-in coordinator durability primitive for the T047 synchronous
replication path. It records the coordinator's intent and phase boundaries so
restart recovery can distinguish an operation that has not started committing
from one that must be reconciled with participants.

## Usage

```go
store, err := hatReplication.NewClusterWriteCommitCoordinatorFileStore(
    hatReplication.ClusterWriteCommitCoordinatorFileStoreOptions{
        Path: "/var/lib/hatrie/cluster-write-coordinator.bin",
    },
)
if err != nil {
    return err
}

result, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(
    ctx, nodes, proposal, prepare, commit, abort, store,
)
```

The existing `ExecuteClusterWriteCommit` API remains the default and has no
state-store work. A caller must explicitly construct a store and use the
`WithStateStore` API.

## Durable Phases

| Phase | Recovery meaning |
| --- | --- |
| `Proposed` | Intent was durably recorded; prepare callbacks have not started. |
| `Prepared` | Every participant prepared; commit has not started. |
| `CommitStarted` | Commit callbacks may be in flight; reconcile by transaction ID. |
| `Committed` | Every commit callback returned success. |
| `AbortStarted` | Prepare failed and abort callbacks are being attempted. |
| `Aborted` | All successful prepares were released. |
| `OutcomeUnknown` | A callback or abort/recovery boundary did not complete; reconcile. |

The store saves `Proposed` before callbacks, `Prepared` before any commit
callback, `CommitStarted` before commit callbacks, and the terminal phase after
callbacks finish. It never performs an unsafe rollback after commit begins.

## File Properties

- Deterministic binary `HCCS1` snapshots inside an `HCCF1` file envelope.
- CRC32C detects torn or corrupted local state; it is not an authentication
  mechanism.
- Maximum snapshot size is 64 MiB by default and is bounded by the constructor.
- Temporary files are created in the destination directory, synced, renamed,
  and followed by a directory sync.
- State files use mode `0600`; newly created parent directories use `0700`.
- `Load` returns `found=false` for a missing file and rejects corrupt state
  without returning a partial snapshot.

## Recovery

1. Load the snapshot during coordinator startup.
2. Treat `CommitStarted` and `OutcomeUnknown` as reconciliation-required.
3. Query each participant using the original transaction ID and proposal.
4. Apply one validated commit or abort decision through the participant
   reconciliation primitive.
5. Keep the terminal coordinator snapshot until an external operational
   checkpoint makes removal safe; call `Remove` only after that checkpoint.

This does not elect a coordinator, dial peers, or automatically reconnect
transports. Those responsibilities remain outside the transport-neutral
primitive and are why parent T047 is still open.

## Measured Cost

`make benchmark-t047i-coordinator-durability` on Linux/amd64, AMD Ryzen 9
5950X, `-benchtime=1s -count=3`:

| Path | Median ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing direct coordinator | 2,389 | 1,264 | 19 | 1.00x |
| In-memory state-store boundaries | 4,686 | 4,144 | 39 | 1.96x |
| File `Save` + `Load` | 1,442,261 | 3,579 | 35 | 603x |
| Full file-backed execution | 5,784,702 | 12,399 | 123 | 2,421x |

The file-backed path performs four durable replacements per successful write.
That cost is the explicit tradeoff for coordinator recovery state and is never
paid by the default API.
