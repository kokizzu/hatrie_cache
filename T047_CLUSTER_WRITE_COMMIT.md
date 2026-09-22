# T047 Cluster Write Commit

`hatReplication.ExecuteClusterWriteCommit` is an opt-in, transport-neutral
two-phase coordinator for writes that must be prepared by every named
participant before any participant is allowed to make the write visible.
`OpenClusterWriteCommitLedger` and
`ExecuteClusterWriteCommitWithLedger` add an opt-in durable participant ledger
for replay protection and post-failure reconciliation.

```go
proposal := hatReplication.ClusterWriteCommitProposal{
	TransactionID: "orders-2026-09-21-0001",
	Sequence:      42,
	FenceToken:    7,
}
result, err := hatReplication.ExecuteClusterWriteCommit(
	ctx,
	[]string{"node-a", "node-b", "node-c"},
	proposal,
	prepare,
	commit,
	abort,
)
if errors.Is(err, hatReplication.ErrClusterWriteCommitOutcomeUnknown) {
	// Reconcile by TransactionID. Do not blindly retry the write.
	_ = result
}
```

For durable outcome state, open one ledger per coordinator process and pass it
to the ledger-aware entrypoint. The file is bounded, CRC-checked, atomically
replaced, and created with owner-only permissions:

```go
ledger, err := hatReplication.OpenClusterWriteCommitLedger(
	"/var/lib/hatrie-cache/quorum-ledger.bin",
	hatReplication.ClusterWriteCommitLedgerOptions{},
)
if err != nil {
	panic(err)
}
defer ledger.Close()
result, err := hatReplication.ExecuteClusterWriteCommitWithLedger(
	ctx, nodes, proposal, prepare, commit, abort, ledger,
)
```

A committed transaction with the same proposal and participant order replays
successfully without callbacks. Any preparing, committing, aborted, or unknown
record returns `ErrClusterWriteCommitLedgerReconcileRequired`; a changed
proposal or participant list returns a conflict instead of reusing the ID.
After reconciliation and any external retention window, call
`ledger.Forget(transactionID)` for committed or aborted records to reclaim the
bounded ledger capacity. Incomplete records cannot be forgotten.

## Safety Contract

- Every node is prepared before the first commit callback starts.
- A prepare failure calls `abort` only for nodes that prepared successfully.
- Once commit starts, the coordinator never calls `abort` or attempts a
  rollback. A partial commit returns `ErrClusterWriteCommitOutcomeUnknown`.
- `TransactionID` is the caller's idempotency and reconciliation key.
- The ledger persists bounded participant state with a versioned CRC-protected
  envelope and atomic replacement. It does not perform network reconciliation.
- The caller owns authenticated transport, participant-side durable writes,
  recovery/reconciliation, and any higher-level membership or consensus
  protocol.
- The existing asynchronous replication and one-phase quorum APIs are
  unchanged. This coordinator has no default or automatic wiring.

Prepare, commit, and abort callbacks must tolerate retries for the same
transaction ID. A commit-phase error is not proof that the participant did not
commit; query participant status before retrying.

## Cost

The in-memory coordinator intentionally performs two callback phases and
allocates a per-participant result. The durable ledger additionally performs
several synchronous, atomically replaced file commits per transaction. In the
measured three-node no-op benchmark, the ledger path was about 12.99 ms,
19.7 KB, and 192 allocations per operation, versus 2.72 us, 1.25 KB, and 18
allocations for the in-memory coordinator. This is a large deliberate cost for
durable replay state, so the ledger remains opt-in and is intended for
correctness-sensitive control-plane writes, not the ordinary data-plane fast
path. The latest benchmark measured about 13.3 ms, 19.7 KB, and 192
allocations per three-node transaction.

Raw samples and the exact reproduction target are recorded in
[BENCHMARK.md](BENCHMARK.md#t047-cluster-write-commit).
