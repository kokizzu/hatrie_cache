# MZ-020: Two-Phase Sink Progress Checkpoints

`hatSql.SQLSinkTwoPhaseCoordinator` is an opt-in protocol for sinks that need
to stage an output before making it visible. It separates external publication
from durable progress acknowledgement:

1. `Prepare` calls the participant's staging operation and durably records a
   `prepared` entry. Prepared progress is not visible through `Frontier`.
2. `Commit` calls the participant's publish operation and then durably records
   the `committed` entry and its sink progress.

The existing one-phase sink APIs are unchanged. The coordinator stores only
bounded transaction metadata and progress, never sink payloads.

## API

```go
coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(ctx,
    hatSql.SQLSinkTwoPhaseOptions{
        Capacity:        1024,
        CheckpointStore: store,
        Name:            "warehouse",
    })

commit := hatSql.SQLSinkCommit{
    Sink:           "warehouse",
    TransactionID:  "txn-42",
    IdempotencyKey: "orders-42",
    Progress: []hatSql.SQLSinkProgress{{
        Sink: "warehouse", Partition: "0", Frontier: 42,
    }},
}

participant := warehouseSinkParticipant{}
if _, err = coordinator.Prepare(ctx, commit, participant); err != nil {
    return err
}
_, err = coordinator.Commit(ctx, commit, participant)
return err
```

`SQLSinkTwoPhaseParticipant.Prepare` must stage without exposing the effect.
Both `Prepare` and `Commit` must be idempotent for the supplied idempotency
key. A commit callback can be retried after a checkpoint-store failure or a
process restart.

`SQLSinkTwoPhaseCheckpointStore.SaveSQLSinkTwoPhaseCheckpoint` must atomically
replace the complete checkpoint and make it durable before returning. A store
implementation should use the same-directory temporary-file, sync, and rename
pattern as the existing file-backed checkpoint stores.

## Recovery Semantics

- A prepared checkpoint is retained after a failed publish or a process crash.
  Recovery constructs the coordinator with the store and retries `Commit`.
- A failed commit does not advance `Frontier`.
- A successful commit advances `Frontier` only after the committed checkpoint
  save succeeds.
- If the process crashes after the external sink publishes but before the
  committed checkpoint is durable, recovery sees `prepared` and calls the
  idempotent participant commit again. The external sink is responsible for
  deduplicating that key.
- This is not a distributed transaction or a rollback mechanism. It narrows
  the crash window and makes the recovery decision durable; it cannot undo an
  effect that was already made visible.
- Prepared entries reserve their sink partitions, so overlapping progress
  cannot be prepared concurrently. Only committed entries are evicted when a
  bounded coordinator reaches capacity.

The protocol is deliberately opt-in because it adds a second callback and a
durable checkpoint write. Existing `Commit` and `SQLSinkExactlyOnceLedger`
users retain their current behavior and performance.

## Verification

Focused correctness tests cover prepare-before-publication ordering, recovery
from a prepared checkpoint, retry after a sink or checkpoint failure, stale and
conflicting progress, failed-prepare retention, and atomic invalid-restore
rejection.

Run:

```text
make test-mz020-two-phase-sink
make race-mz020-two-phase-sink
make vet-mz020-two-phase-sink
make benchmark-mz020-two-phase-sink
```

The benchmark compares the existing in-memory one-phase exactly-once ledger
with the new two-phase coordinator. It excludes storage I/O and sink work so
the coordinator overhead is isolated.
