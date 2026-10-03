# T047h Durable Coordinator Decisions

T047h adds an opt-in durable decision record around
`hatReplication.ExecuteClusterWriteCommit`. The existing two-phase coordinator
can already prepare participants and report an indeterminate commit. This
feature preserves the coordinator's last stable phase so a restarted process
can find work that needs reconciliation instead of losing the transaction ID.

## API

```go
store, err := hatReplication.NewClusterWriteCommitDecisionFileStore(
	hatReplication.ClusterWriteCommitDecisionFileStoreOptions{
		Path: "/var/lib/hatrie/cluster-write-decisions.state",
	},
)
if err != nil {
	return err
}

result, err := hatReplication.ExecuteClusterWriteCommitDurable(
	ctx,
	[]string{"node-a", "node-b", "node-c"},
	proposal,
	prepare,
	commit,
	abort,
	store,
)
```

The recorder writes these phases in order:

1. `Preparing` before participant prepare callbacks start.
2. `Prepared` after every participant has prepared.
3. `Committing` immediately before commit callbacks start.
4. `Committed`, `Aborted`, or `Indeterminate` after the corresponding outcome.

The existing `ExecuteClusterWriteCommit` function remains the default and does
not construct or write a decision record. `ExecuteClusterWriteCommitDurable`
rejects a nil recorder rather than silently weakening its durability contract.

## Recovery

After restart, open the same file store and call `List` or `Load`.

- `Aborted` means no commit callback was started; the record can be retained
  for audit and deleted after the caller's retention period.
- `Committed` means every commit callback returned success and can be used to
  finish local cleanup.
- `Indeterminate` means at least one commit callback started without every
  participant confirming success. Query each participant by the stable
  `Proposal.TransactionID`, then apply the authoritative outcome through the
  existing participant `Reconcile` API. Do not blindly retry the write.

`Delete` is explicit and caller-owned. The coordinator never removes evidence
needed to reconcile a partial commit. The file store is intended to have one
coordinator owner per path; atomic replacement protects readers and crash
recovery, but it is not a cross-process lock.

## Format And Safety

- `HCD1` is the deterministic binary decision snapshot.
- `HCDS1` wraps it with a bounded length and CRC32C checksum.
- Transaction IDs are bounded to 1 MiB, node IDs to 64 KiB, participants to
  `MaxClusterWriteCommitNodes`, records to the configured `MaxRecords`, and
  the file to the configured `MaxBytes` (64 MiB by default).
- Records are sorted by transaction ID before encoding; participant input
  order is retained and duplicate nodes are rejected.
- Loads check the file size before reading and validate the complete frame
  before exposing any record.
- New parent directories use mode `0700`; temporary and final state files use
  mode `0600` and are fsynced before atomic replacement.
- CRC32C detects accidental truncation or corruption. It is not authentication
  or encryption; protect the directory and use authenticated transport when
  decision records cross a host boundary.

The record contains the transaction identity, sequence, fence token, payload
digest, participant names, and phase. It does not contain application payloads.

## Measured Tradeoff

Reproduce the after-change benchmark with:

```text
make benchmark-tu47-coordinator-decision
```

Linux/amd64 on an AMD Ryzen 9 5950X. The existing coordinator was measured on
clean `origin/master` and the legacy path was rerun after the change with
`-benchtime=200ms -count=5`:

| Path | Median time | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Existing coordinator, before | 2,466 ns | 1,248 | 18 | Baseline |
| Existing coordinator, after | 2,641 ns | 1,248 | 18 | 0.93x baseline time, same memory |
| Durable coordinator, one transaction | 6.60 ms | 14,480 | 125 | About 2,700x slower than baseline |

The durable path is therefore intentionally opt-in. The large latency cost is
filesystem durability, not an accidental allocation regression in the normal
coordinator; repeated runs varied from about 5.5 ms to 44.6 ms as filesystem
sync latency changed. Use the durable API for correctness-sensitive
control-plane transactions where crash recovery is worth several sync
boundaries; keep ordinary writes on the existing API.

Focused tests, the full `hatReplication` package, race detection, and `go vet`
cover success, prepare failure, indeterminate commit recovery, phase
regressions, participant identity conflicts, CRC corruption, and oversized
file rejection.
