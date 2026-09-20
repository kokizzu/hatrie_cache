# T-U09 Snapshot-plus-WAL Join Bootstrap

T-U09 adds an importable, caller-driven bootstrap coordinator for adding a
replica from an online snapshot and the journal records after that snapshot.
It is a Tarantool-style operational primitive, not an automatic cluster
membership or network protocol.

## Contract

`hatBackup.NewSnapshotJoinBootstrap` validates a bounded manifest containing:

- source and joiner IDs;
- a positive fencing token;
- the snapshot sequence and inclusive WAL end sequence; and
- a 32-byte snapshot checksum.

The state machine is:

```text
pending -> snapshot_applied -> ready -> active
             \-> failed       \-> failed
```

The snapshot callback runs once. WAL batches must start at the next sequence,
contain contiguous records, stay within the manifest end, and stay within the
configured record and batch limits. The coordinator copies record payloads
before invoking the caller's batch callback, so the transfer buffer can be
reused after the callback returns.

Activation is rejected unless the complete WAL interval is applied and the
caller supplies the manifest fencing token. Snapshot, WAL, and activation
callback failures are terminal because the caller may have partially changed
the destination; the caller can discard the joiner and restart with a new
coordinator.

## Resume

`Checkpoint` returns stable state after a callback completes. Its `SJC1`
binary encoding contains the manifest, phase, applied sequence, and a bounded
failure string, followed by a CRC32C. `RestoreCheckpoint` requires the same
manifest and only accepts stable phases. It does not persist snapshot or WAL
payloads, and it does not grant activation authority.

```go
join, err := hatBackup.NewSnapshotJoinBootstrap(manifest, hatBackup.SnapshotJoinOptions{})
if err != nil {
    return err
}
if err := join.ApplySnapshot(ctx, manifest.SnapshotChecksum, installSnapshot); err != nil {
    return err
}
if err := join.ApplyWAL(ctx, records, applyWALBatch); err != nil {
    return err
}
return join.Activate(ctx, manifest.FencingToken, publishReplica)
```

The callbacks own the actual file transfer, destination transaction, journal
replay, fencing publication, and cleanup. No goroutine, network listener, or
automatic cluster membership starts by default.

## Measured Cost

Measured with `make benchmark-tu09-baseline` on the clean pre-feature commit
and `make benchmark-tu09` on the feature branch, Linux/amd64, AMD Ryzen 9
5950X, five samples per benchmark:

| Workload | Median | Memory | Allocations | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Direct two-record sequence control | 2.607 ns/op | 0 B/op | 0 | Uncoordinated baseline |
| Snapshot plus two-record WAL coordinator | 201.1 ns/op | 240 B/op | 4 | Includes locking, validation, and payload copies |
| SJC1 checkpoint encode/decode | 294.7 ns/op | 288 B/op | 3 | Explicit resume/verification path |

The coordinator is intentionally opt-in, so these costs are not paid by
ordinary writes, journal replay, or backup creation. The direct control is not
an apples-to-apples replacement: it omits callbacks, state transitions,
caller-buffer isolation, manifest checks, and fencing. Its purpose is to make
the incremental coordination cost visible rather than claim a throughput
improvement.
