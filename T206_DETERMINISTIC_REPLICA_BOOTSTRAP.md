# T206 Deterministic Replica Bootstrap

`hatReplication` now provides an importable admission and lifecycle wrapper
for snapshot-plus-WAL replica joins. It fills the gap between a set of source
advertisements and the existing transport-neutral
`SnapshotWALBootstrapCoordinator`.

## Usage

Source selection and workflow construction can be done in one call:

```go
request := hatReplication.ReplicaBootstrapRequest{
    JoinerID:                  "node-b",
    RequiredStorageGeneration: 7,
    MinimumJournalSequence:    100,
    PreferredRegions:          []string{"sg"},
    MaxWALGap:                 20,
}
workflow, err := hatReplication.NewReplicaBootstrapWorkflowFromSources(request,
    []hatReplication.ReplicaBootstrapSource{
        {
            NodeID:                  "node-a",
            Address:                 "10.0.0.1:9000",
            Region:                  "sg",
            Ready:                   true,
            SnapshotID:              "snapshot-42",
            StorageGeneration:       7,
            SnapshotJournalSequence: 100,
            CurrentJournalSequence:  112,
            FencingToken:            9,
        },
    },
)
if err != nil {
    return err
}

state, err := workflow.Begin()
if err != nil {
    return err
}
plan := workflow.Plan().Bootstrap

// After the caller has copied and verified plan.SnapshotID, storage generation,
// and journal boundary:
state, err = workflow.Coordinator().InstallSnapshot(
    plan.SnapshotID,
    plan.StorageGeneration,
    plan.SnapshotJournalSequence,
    plan.FencingToken,
)
```

The caller owns the actual snapshot transfer, journal replay, integrity checks,
health checks, and topology publication. The workflow only admits a plan and
fences the lifecycle transitions; it does not open a network listener or copy
files automatically.

## Deterministic Selection

`PlanReplicaBootstrap` rejects duplicate source identities and excludes the
joiner itself, unready sources, storage-generation mismatches, stale sources,
invalid snapshot boundaries, zero storage generations, and zero fencing tokens.
The effective default `MaxWALGap` is `1 << 20`; values above `1 << 32` are
rejected. A non-zero requested storage generation is an exact match.

Among eligible sources, the stable ordering is:

1. Preferred region order.
2. Highest current journal sequence.
3. Highest snapshot journal sequence.
4. Highest storage generation.
5. Lexically smallest source node ID.
6. Lexically smallest snapshot ID.

Source input order therefore cannot change the selected source or bootstrap
boundary. The returned plan carries both the selected source advertisement and
the exact `SnapshotWALBootstrapPlan` consumed by the fenced lifecycle.

## Safety Boundary

The plan validates node and snapshot identifiers with the same bounded,
NUL-rejecting rules as the existing bootstrap coordinator. Source identity is
deduplicated before selection. `InstallSnapshot`, `AdvanceWAL`, `MarkReady`,
and `Activate` still require the current fencing token and, where applicable,
the current generation. A source cannot be activated until the target journal
sequence has been applied.

This is opt-in. Existing replication, checkpoint bootstrap, and command paths
keep their current behavior until a caller uses the new API.

## Measurements

Measurements used 64 source advertisements on an AMD Ryzen 9 5950X with
`go test -benchmem -count=5`:

| Path | Median | Heap | Allocations | Notes |
| --- | ---: | ---: | ---: | --- |
| Existing first-eligible scan | 5.98 ns/op | 0 B/op | 0 | Does not rank deterministically or reject duplicate IDs |
| Deterministic map prototype | 5.30 us/op | 3,496 B/op | 3 | Rejected as unnecessary heap cost |
| Final bounded stack hash table | 2.73 us/op | 0 B/op | 0 | Selected implementation |

The final planner is about 1.94x faster than the deterministic map prototype
and removes all measured planner heap allocation. The first-eligible scan is
not a semantic equivalent and is shown only as a lower-bound comparison. The
planner runs during replica admission, not per-command replication or query
execution.

## Verification

```text
make test-t206
make test-t206-package
make race-t206
make vet-t206
make benchmark-t206
```
