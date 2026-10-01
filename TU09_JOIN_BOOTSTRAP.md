# T-U09 Join Bootstrap

`hatReplication.JoinBootstrapState` is the reusable control-plane contract for
a snapshot-plus-WAL replica join. It makes the dangerous boundaries explicit:
the target starts at a snapshot journal sequence, reports monotonic WAL
progress, proves it reached the source fence, and only then enters activation.

The state is a small copyable value. It has no goroutines, maps, network calls,
or global registry, so it is safe to persist as JSON beside an operator's join
workflow and resume after a failed process.

## Lifecycle

1. Create a planned state with an operation ID, source ID, target node ID,
   positive topology fencing token, and installed snapshot sequence.
2. Call `BeginCatchUp` before applying WAL batches.
3. Call `RecordApplied` after each successful batch. Equal reports are
   idempotent; lower reports are rejected.
4. Call `PrepareActivation` with the current fencing token and source sequence.
   It rejects stale topology generations and targets that have not caught up.
5. Publish the topology atomically in the caller's storage/control plane.
6. Call `Activate` after publication and verification.
7. On a pre-activation failure, call `Abort`; after cleanup, `Retry` resumes
   from the already-installed snapshot and applied sequence.

```go
state, err := hatReplication.NewJoinBootstrapState(
    "join-2026-10-01-node-b",
    "node-a",
    "node-b",
    topology.FencingToken,
    snapshot.JournalSequence,
)
state, err = state.BeginCatchUp()
state, err = state.RecordApplied(pull.AppliedThrough)
state, err = state.PrepareActivation(topology.FencingToken, source.Sequence)
// Caller atomically publishes the topology here.
state, err = state.Activate(topology.FencingToken)
```

The example intentionally leaves network and topology publication to the
caller. The package does not authenticate peers, decide quorum, copy snapshot
bytes, or claim that a remote response is truthful. Use the existing
authenticated monitoring/replication transport and persist the JSON state with
the same access controls as the topology and journal state.

## Safety Properties

- A state is bound to one positive fencing token. A changed topology generation
  must create a new state instead of reusing a stale one.
- Snapshot and WAL coordinates are monotonic and duplicate-safe.
- `PrepareActivation` requires `AppliedSequence >= sourceSequence`.
- Activation is idempotent for the same token; stale tokens are rejected.
- Abort is allowed before activation and retains enough state for retry.
- Activated state cannot be aborted, preventing an operator from silently
  hiding a membership publication that already completed.

The coordinator is opt-in and has no effect on existing replication paths until
a caller uses it.
