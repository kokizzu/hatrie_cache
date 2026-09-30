# M227 Source Snapshot Frontier

M227 couples a source snapshot's first live-stream frontier to the same
checkpoint that stores its rows, snapshot ID, and source offsets. This closes a
cutover ambiguity: a recovered snapshot no longer has to reconstruct its first
live position from separate mutable state.

## Contract

`hatSql.SQLExternalSnapshotProvider.Snapshot` returns
`SQLExternalSnapshotMetadata`. A provider that knows the source's live-stream
boundary sets:

```go
metadata := hatSql.SQLExternalSnapshotMetadata{
    Source:            "orders-source",
    Key:               "orders",
    Kind:              "POSTGRES",
    SnapshotID:        "snapshot-2026-09-30T12:00:00Z",
    FirstLiveFrontier: 42,
    FirstLiveFrontierSet: true,
    Offsets: []hatSql.SQLExternalSnapshotOffset{
        {Source: "orders-source", Partition: "wal", Offset: 41},
    },
}
```

`FirstLiveFrontierSet` is intentional. Frontier `0` can be a valid source
position, so a zero value alone cannot distinguish “frontier is zero” from
“older provider/checkpoint did not supply the field.” Legacy payloads with the
bit unset remain readable. A consumer that requires an exact cutover should
require the bit and take a fresh snapshot when it is absent.

The marker is the first live-stream frontier after snapshot cutover. It is not
a wall-clock timestamp, a current lag measurement, or a replacement for the
per-partition offsets. Offsets identify the snapshot's completed source data;
the frontier identifies where the live stream begins after that snapshot.

## Atomicity

For single-source ingestion, the ingestor validates and installs rows, offsets,
snapshot ID, frontier value, and the set bit together. `Commit` receives one
`SQLExternalSnapshot`, so a checkpoint implementation must publish all of the
fields atomically. A failed commit restores the previous in-memory snapshot,
including the set bit.

For `SQLMultiSourceSnapshotCoordinator`, each source retains its own frontier
through sorting, cloning, coordinated commit, and recovery. The coordinator
does not invent a global frontier from independent source positions.

## Safe Consumer Handoff

After recovery, a connector should:

1. Verify `FirstLiveFrontierSet` when exact cutover is required.
2. Restore the source offsets from the same snapshot.
3. Initialize the live consumer at the source-defined first-live frontier and
   apply the connector's documented inclusive/exclusive offset rule.
4. Persist later live progress using the same ordering and integrity rules as
   the initial checkpoint.

The inclusive/exclusive rule is source-specific and remains the connector's
responsibility. The library stores the boundary; it does not interpret Kafka,
WAL, or CDC semantics.

## Compatibility And Security

The added fields are bounded scalar metadata and do not increase row or page
limits. Existing authentication, identity validation, offset validation, and
checkpoint ownership rules remain in force. Providers must not put credentials,
connection strings, or secrets into snapshot metadata. Checkpoint stores should
protect the payload with the same access controls, encryption, integrity check,
and atomic replacement policy used for the rest of the snapshot.

## Verification

The focused tests cover capture, checkpoint recovery, multi-source sorting and
cloning, exact frontier `0`, and rollback of the set bit after a failed commit.

```text
make test-m227-relevant
make test-m227-relevant-race
```

The M227 benchmark compares the existing snapshot control paths before and
after adding the scalar metadata. It measures control-plane capture, recovery,
and resolve; ordinary SQL row reads are unchanged.
