# Snapshot-plus-WAL Join Bootstrap

`hatReplication.JoinBootstrap` is the safety gate for adding a replica from a
snapshot and then replaying the journal. It keeps the transport and storage
operations caller-owned while making the required order explicit:

1. Create or receive a snapshot whose journal sequence is known.
2. Install that exact snapshot on the target.
3. Replay the journal through a monotonic sequence at least as new as the snapshot.
4. Activate the target with the exact non-zero fencing token selected by the control plane.

The state machine is importable from `hatrie_cache/hat/hatReplication`:

```go
bootstrap, err := hatReplication.NewJoinBootstrap(hatReplication.JoinBootstrapOptions{
	SourceNode:       "primary-a",
	TargetNode:       "replica-b",
	SnapshotSequence: manifest.JournalSequence,
	FencingToken:     topology.FencingToken,
})
if err != nil {
	return err
}
if err := bootstrap.SnapshotInstalled(manifest.JournalSequence); err != nil {
	return err
}
if err := bootstrap.CatchUp(pullResult.AppliedThrough); err != nil {
	return err
}
if _, err := bootstrap.Activate(pullResult.AppliedThrough, topology.FencingToken); err != nil {
	return err
}
```

`SnapshotInstalled` rejects a stale or mismatched checkpoint. `CatchUp` rejects
regression and cannot run before the snapshot. `Activate` rejects stale fencing
tokens and cannot run before catch-up. Repeating a successful snapshot, catch-up,
or activation report is safe; `Abort` makes a failed join terminal and records
the reason.

The existing monitoring APIs provide the transport pieces: the source can serve
`GET /api/journal/checkpoint`, and the target can use the existing checkpoint
adoption/bootstrap path before `PullCommandJournal` catches up the remaining
WAL. This package does not silently change the existing CLI workflow or enable
remote snapshot transfer by default.

## Operational Boundary

The fencing token must be allocated and persisted by the topology/control plane.
Callers must carry it on replication writes and publish membership only after
`Activate` succeeds. This helper is not consensus and does not fence a process
by itself; it prevents a caller from skipping or reordering the local join
contract.
