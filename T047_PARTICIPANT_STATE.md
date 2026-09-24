# T047 Durable Participant State

`ClusterWriteCommitParticipant` is the opt-in local state machine for the
participant side of [T047 cluster write commit](T047_CLUSTER_WRITE_COMMIT.md).
It is importable from `hatReplication` and does not open sockets, start a
goroutine, or change the existing asynchronous replication and one-phase
quorum defaults.

## What It Does

The state machine retains a bounded record for each transaction ID:

1. `Prepare` validates and records the proposal without making the write
   visible.
2. `Commit` transitions the prepared proposal to committed.
3. `Abort` transitions the prepared proposal to aborted.
4. `Status` returns a copy for reconciliation after an unknown coordinator
   outcome.

Repeating the same phase request is idempotent. A different proposal with the
same transaction ID is rejected. Committed and aborted phases are terminal.
The state machine is only the participant ledger; the caller still applies the
actual write and persists the application data using its own durable path.

## API Example

```go
participant, err := hatReplication.NewClusterWriteCommitParticipant(
	 hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1024},
)
if err != nil {
	 panic(err)
}

proposal := hatReplication.ClusterWriteCommitProposal{
	TransactionID: "orders-2026-09-21-0001",
	Sequence:      42,
	FenceToken:    7,
}

if _, err := participant.Prepare(proposal); err != nil {
	// Return the prepare error to the coordinator.
}

// The commit callback applies the write and then records the terminal phase.
if _, err := participant.Commit(proposal); err != nil {
	// Reconcile with participant.Status(proposal.TransactionID).
}

snapshot, err := participant.MarshalSnapshot()
if err != nil {
	panic(err)
}

recovered, err := hatReplication.NewClusterWriteCommitParticipant(
	 hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1024},
)
if err != nil {
	panic(err)
}
if err := recovered.RestoreSnapshot(snapshot); err != nil {
	panic(err)
}
```

Wire the `Prepare`, `Commit`, and `Abort` calls into the callbacks passed to
`ExecuteClusterWriteCommit`. Persist `MarshalSnapshot` output with the
application's journal or checkpoint mechanism at the same durability boundary
as the participant's application data. A participant snapshot alone does not
make an application write crash-safe.

## Snapshot Format And Bounds

`MarshalSnapshot` emits deterministic `HCP1` binary data. Records are sorted by
transaction ID, so map iteration order does not affect the bytes. Each record
contains the transaction ID, sequence, fence token, 32-byte payload digest,
and terminal/prepared phase. `RestoreSnapshot` validates the complete payload
before atomically replacing the in-memory map; malformed or trailing data
cannot partially mutate live state.

The default limit is 1,024 records. `MaxRecords` can be lowered or raised up
to `MaxClusterWriteCommitParticipantRecords` (1,048,576). Snapshots are
limited to 64 MiB and transaction IDs to 1 MiB. These bounds prevent a
recovery payload from requesting unbounded map growth. Raising `MaxRecords`
intentionally increases the possible retained memory and should follow the
deployment's transaction timeout and reconciliation policy.

The implementation uses a mutex for concurrent callers and copies records out
before encoding. Snapshot encoding and restore are control-plane operations;
they are not on the ordinary write fast path unless the caller explicitly
invokes them.
