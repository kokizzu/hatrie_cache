# T-U09 Snapshot-plus-WAL Join Bootstrap

`hatReplication.SnapshotWALBootstrapCoordinator` is a bounded, transport-neutral
state machine for bringing a new replica from an immutable snapshot to an exact
journal boundary. It closes the lifecycle gap between installing a snapshot,
replaying the WAL, declaring readiness, and activating a joiner.

The coordinator is deliberately caller-owned at the I/O boundary. It does not
copy files, read journal records, elect a leader, or publish a topology member.
Callers perform each side effect successfully and then advance the coordinator.

## Lifecycle

1. `Begin` validates one immutable plan and enters `snapshot_pending`.
2. `InstallSnapshot` confirms the exact snapshot ID, storage generation, and
   snapshot journal sequence after the caller installs the snapshot.
3. `AdvanceWAL` records a contiguous `applied through` sequence. It is
   idempotent for repeated progress and stores no WAL payloads.
4. `MarkReady` requires the target sequence and the current lifecycle generation.
5. `Activate` requires the ready generation and the same fencing token.

`Abort` can stop a pending, catching-up, or ready join with a bounded reason.
Each coordinator is single-use; construct a new one for a different snapshot.

```go
coordinator, err := hatReplication.NewSnapshotWALBootstrapCoordinator(
	hatReplication.SnapshotWALBootstrapOptions{},
)
if err != nil {
	 return err
}

state, err := coordinator.Begin(hatReplication.SnapshotWALBootstrapPlan{
	JoinerID:                 "node-b",
	SourceID:                 "node-a",
	SnapshotID:               "snapshot-7",
	StorageGeneration:       4,
	SnapshotJournalSequence: 100,
	TargetJournalSequence:   103,
	FencingToken:             9,
})
if err != nil {
	return err
}

// Install and validate the immutable snapshot before this call.
state, err = coordinator.InstallSnapshot("snapshot-7", 4, 100, 9)
if err != nil {
	return err
}

// Apply records 101..103 contiguously, then report the highest applied record.
state, err = coordinator.AdvanceWAL(103, 9)
if err != nil {
	return err
}
state, err = coordinator.MarkReady(state.Generation, 9)
if err != nil {
	return err
}
_, err = coordinator.Activate(state.Generation, 9)
```

## Safety Contract

The fencing token is required to be non-zero and must match every transition.
This prevents a stale source or join worker from activating after a newer
topology handoff. The lifecycle generation changes on every state or progress
change, so stale operator actions cannot mark a newer replay state ready or
active.

The default maximum snapshot-to-WAL gap is 1,048,576 records. Set
`MaxWALGap` explicitly for a larger recovery envelope, up to the hard limit of
4,294,967,296. This limit bounds stale-plan admission; the coordinator itself
does not retain the records or allocate per-record state.

`AdvanceWAL` means that the caller has applied every record through the supplied
sequence. The coordinator cannot prove that property without owning the WAL, so
the caller must not skip unapplied records. `Activate` changes only the local
coordinator state; the embedding control plane must perform its own atomic
topology/publication update using the same generation and fencing token.

Do not accept joiner IDs, snapshot IDs, sequences, or fencing tokens directly
from an untrusted client. Authentication, authorization, snapshot checksums,
WAL integrity, retry cleanup, and cross-process fencing remain the caller's
responsibility.

## Measurement

Five `-benchmem` samples on an AMD Ryzen 9 5950X, Linux/amd64:

| Operation | Median | Allocations | Reference |
| --- | ---: | ---: | --- |
| Simple atomic sequence baseline | 1.875 ns/op | 0 B/op, 0 allocs/op | Lower bound only |
| Idempotent `AdvanceWAL` admission | 26.33 ns/op | 0 B/op, 0 allocs/op | ~14.0x baseline |
| `Snapshot` status read | 13.10 ns/op | 0 B/op, 0 allocs/op | ~7.0x baseline |

The cost is bounded mutex-protected control-plane validation. No background
worker, WAL buffer, or per-record allocation is introduced. The simple atomic
loop is not a full join baseline; actual snapshot installation and WAL replay
dominate the end-to-end bootstrap time.

Run the reproducible benchmark with:

```text
make benchmark-tu09-dev
```

Raw samples are included in [BENCHMARK.md](BENCHMARK.md#t-u09-snapshot-plus-wal-bootstrap-coordinator).
