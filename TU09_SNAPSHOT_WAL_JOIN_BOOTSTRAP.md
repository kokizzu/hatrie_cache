# T-U09: Snapshot-plus-WAL Join Bootstrap

Status: partially adopted as an opt-in, transport-neutral control-plane
coordinator.

`hatReplication.SnapshotWALBootstrapCoordinator` makes a new replica join
explicitly pass through these states:

1. Register a node, source, non-zero fencing token, and exact snapshot sequence.
2. Acknowledge installation of that exact snapshot.
3. Advance the source and applied WAL sequences monotonically.
4. Fence the source and capture its final sequence.
5. Activate only after the replica has applied through that final sequence.

The activation transition is protected by the coordinator mutex and is
idempotent for retries with the same fencing token. The coordinator retains
only bounded metadata; it does not copy snapshot bytes, transfer WAL records,
authenticate peers, persist join state, or change cluster membership by itself.

## Go API

```go
coordinator, err := hatReplication.NewSnapshotWALBootstrapCoordinator(
    hatReplication.SnapshotWALBootstrapOptions{},
)
if err != nil {
    return err
}

_, err = coordinator.Begin(hatReplication.SnapshotWALBootstrapRequest{
    ID:               "join-1",
    Node:             "node-b",
    Source:           "node-a",
    FencingToken:     7,
    SnapshotID:       "snapshot-7",
    SnapshotSequence: 100,
})
if err != nil {
    return err
}

// The caller transfers and verifies the actual snapshot before this step.
if err := coordinator.MarkSnapshotInstalled("join-1", "snapshot-7", 100); err != nil {
    return err
}
if err := coordinator.AdvanceSource("join-1", 7, 120); err != nil {
    return err
}
if err := coordinator.AdvanceApplied("join-1", 7, 120); err != nil {
    return err
}
if _, err := coordinator.FenceSource("join-1", 7); err != nil {
    return err
}
activation, err := coordinator.Activate("join-1", 7)
if err != nil {
    return err
}
_ = activation.Sequence // 120: the exact source boundary now activated.
```

`Status` returns a detached session snapshot. `Statuses` returns detached
snapshots sorted by session ID, which makes diagnostics deterministic.

## Defaults and limits

- Constructing the coordinator is opt-in. No global coordinator or automatic
  cluster-join behavior is enabled.
- `MaxSessions: 0` selects 64 retained sessions.
- A coordinator accepts at most 4,096 sessions.
- IDs and labels are limited to 256 bytes and reject leading/trailing spaces and
  control characters. Abort reasons are limited to 512 bytes.
- A non-zero fencing token is required. Token validation is local; the caller
  must obtain it from an authenticated, authoritative membership system.

## Failure behavior

- Snapshot IDs and snapshot sequences must match the `Begin` request exactly.
- Source and applied sequences cannot move backward.
- Applied progress cannot move ahead of the observed source sequence.
- A stale fencing token is rejected before a state transition.
- Source progress is rejected after `FenceSource`.
- Activation before snapshot installation, source fencing, or catch-up returns a
  specific sentinel error.
- Repeating the same snapshot acknowledgement, abort, or activation is safe;
  conflicting retries are rejected.
- Aborted sessions cannot be activated or reused.

## Deployment workflow

The transport and cluster manager should own the complete operation:

1. Authenticate the joining node and obtain the current source fencing token.
2. Create a consistent snapshot and record its exact journal sequence and
   digest.
3. Transfer the verified snapshot and replay journal records after that
   sequence into the joining node.
4. Feed source and applied progress to the coordinator.
5. Stop or fence source writes through the cluster manager, call
   `FenceSource`, finish the bounded WAL tail, then call `Activate`.
6. Publish the activation result through the cluster manager and remove or
   retain the session according to the operator's durable membership policy.

The coordinator does not replace authenticated transport, durable membership
metadata, consensus, a real source-write fence, or atomic publication of the
new node in service discovery. Those boundaries are deliberate so callers can
choose their existing replication and backup mechanism without silently
changing the default deployment model.

## Measurement

Command:

```text
make benchmark-chg08-bootstrap
```

Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. The
direct path is an unsafe sequence-and-fence check used only as a cost control;
it does not provide bounded sessions, stale-token rejection, retry semantics,
or atomic state transitions.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Direct sequence checks | 0.2302 | 0 | 0 | 1.00x control |
| Full coordinator workflow | 447.9 | 496 | 4 | 1,946x control cost |

The 447.9 ns and 496 B cost is paid once per join-control workflow, not per
snapshot byte or WAL record. This feature is a correctness and operational
safety contract, not a performance optimization; the benchmark is reported as
overhead rather than an improvement claim.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
direct_sequence_checks: 0.2237/0/0; 0.2229/0/0; 0.2303/0/0; 0.2302/0/0; 0.2307/0/0
coordinator_workflow:   447.9/496/4; 464.0/496/4; 437.6/496/4; 439.2/496/4; 448.8/496/4
```
