# T047 Cluster Write Commit

`hatReplication.ExecuteClusterWriteCommit` is an opt-in, transport-neutral
two-phase coordinator for writes that must be prepared by every named
participant before any participant is allowed to make the write visible.

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

## Safety Contract

- Every node is prepared before the first commit callback starts.
- A prepare failure calls `abort` only for nodes that prepared successfully.
- Once commit starts, the coordinator never calls `abort` or attempts a
  rollback. A partial commit returns `ErrClusterWriteCommitOutcomeUnknown`.
- `TransactionID` is the caller's idempotency and reconciliation key.
- The caller owns authenticated transport, durable participant state,
  recovery/reconciliation, and any higher-level membership or consensus
  protocol.
- The existing asynchronous replication and one-phase quorum APIs are
  unchanged. This coordinator has no default or automatic wiring.

Prepare, commit, and abort callbacks must tolerate retries for the same
transaction ID. A commit-phase error is not proof that the participant did not
commit; query participant status before retrying.

## Cost

The coordinator intentionally performs two callback phases and allocates a
per-participant result. In a local no-op callback benchmark it is roughly 2x
the time and 2.3x the transient bytes of the existing one-phase quorum path.
That cost buys a prepare barrier and an explicit indeterminate-outcome state;
the API is intended for correctness-sensitive control-plane writes, not the
ordinary data-plane fast path.

Raw samples and the exact reproduction target are recorded in
[BENCHMARK.md](BENCHMARK.md#t047-cluster-write-commit).
