# T207 Replica Recovery and Rejoin

T207 adds a bounded, generation-fenced protocol for replica eviction, rejoin,
and stale-state recovery. It records the latest incarnation and applied
sequence for each node, keeps evicted identities as tombstones, and returns a
side-effect-free plan that says whether a node may resume WAL replay or must
bootstrap from a fresh snapshot.

The protocol is intentionally a control-plane primitive. It does not open a
network connection, transfer a snapshot, replay WAL records, mutate cluster
topology, or provide consensus. Callers should persist and replicate its
generation/fencing decisions through the existing membership or consensus
mechanism before publishing them.

## Safety Rules

- Node IDs are normalized and must be non-empty.
- Incarnations must be non-zero. A process with an older incarnation cannot
  rejoin.
- A live node cannot silently replace its incarnation. Evict the old
  incarnation first, then rejoin with a strictly newer incarnation.
- Evicted nodes remain tombstones, preventing an old process from reappearing
  after a crash or network partition.
- Rejoin requests beyond the source journal sequence or storage generation are
  rejected as future state.
- A node whose storage generation differs from the source, or whose applied
  sequence is below the source's retained WAL boundary, receives a bootstrap
  plan.
- Mutations require the exact current registry generation and a strictly newer
  fencing token. A plan whose generation or fence is stale cannot commit.
- The registry is bounded by `ReplicaRecoveryOptions.MaxNodes`, including
  tombstones, so repeated churn cannot grow memory without limit.

## Basic Flow

```go
protocol, err := hatReplication.NewReplicaRecoveryProtocol(
    hatReplication.ReplicaRecoveryOptions{MaxNodes: 1024},
)
if err != nil {
    return err
}

state, err := protocol.Admit(hatReplication.ReplicaRecoveryNode{
    NodeID:            "replica-eu-1",
    Incarnation:       1,
    StorageGeneration: 7,
}, 0, 1)
if err != nil {
    return err
}

plan, err := protocol.EvaluateRejoin(
    hatReplication.ReplicaRejoinRequest{
        NodeID:              "replica-eu-1",
        Incarnation:         1,
        LastAppliedSequence: 120,
        StorageGeneration:   7,
    },
    hatReplication.ReplicaRecoverySource{
        CurrentJournalSequence: 200,
        RetainedFromSequence:   100,
        StorageGeneration:      7,
    },
)
if err != nil {
    return err
}

switch plan.Decision {
case hatReplication.ReplicaRecoveryDecisionResume:
    // Replay from plan.RequiredSequence through the source frontier.
case hatReplication.ReplicaRecoveryDecisionBootstrap:
    // Install a fresh snapshot, then replay WAL through the source frontier.
default:
    return errors.New("rejoin rejected")
}

_, err = protocol.CommitRejoin(
    plan,
    200, // sequence applied after resume or bootstrap
    7,   // installed storage generation
    plan.FencingToken,
)
if err != nil {
    return err
}
_ = state
```

`EvaluateRejoin` does not reserve a slot or change state. The caller performs
the selected transfer, then calls `CommitRejoin`. If another membership or
recovery mutation occurs first, the commit fails with a generation or fencing
error and the caller must evaluate a new plan.

## Eviction and Reincarnation

```go
state, err = protocol.Evict("replica-eu-1", state.Generation, 2)
if err != nil {
    return err
}

// The old incarnation is rejected. A new process must use incarnation 2 or
// higher and must commit through EvaluateRejoin plus CommitRejoin.
_, err = protocol.Admit(hatReplication.ReplicaRecoveryNode{
    NodeID:      "replica-eu-1",
    Incarnation: 2,
}, state.Generation, 3)
```

`Admit` deliberately does not replace an evicted identity directly. This
prevents an operator or stale process from bypassing the rejoin checks. The
caller can compact tombstones only after its external membership authority has
proved that the old identities cannot return; T207 does not make that proof on
its own.

## Persistence and Operations

Persist the returned snapshots or the enclosing membership journal using the
same durable commit path as topology changes. A process restart must restore
the latest generation, fencing token, active nodes, and eviction tombstones
before accepting rejoin requests. The protocol's in-memory map is not a
durability mechanism.

The registry is safe for concurrent `EvaluateRejoin` and `Snapshot` calls, and
serializes `Admit`, `Evict`, and `CommitRejoin`. The detached snapshot is sorted
by node ID and can be safely retained or serialized by the caller.

## Verification

```text
make test-t207
make test-t207-package
make race-t207
make vet-t207
make benchmark-t207
```

See [BENCHMARK.md](BENCHMARK.md#t207-replica-eviction-rejoin-and-stale-state-recovery)
for the measured decision and snapshot costs.
