# CH-019 Replicated-Part Checks

`hatMerkle.PartManifest` supplies whole-part and per-column checksums. CH-019
adds the missing replica-level consistency workflow: compare two verified
active-part snapshots and produce a deterministic repair plan.

## Scope

The planner is intentionally transport-neutral. It does not open files, copy
bytes, delete data, contact peers, or publish a replica. The embedding
replication layer owns those side effects and should apply an action only after
rechecking the source and target generations with `ReplicaPartRepairPlan.IsCurrent`.

Target-only parts are reported as `quarantine`, not `delete`. This preserves a
rollback path and makes cleanup an explicit operator decision.

## Example

```go
sourceParts, _ := sourceCatalog.Snapshot()
targetParts, _ := targetCatalog.Snapshot()

source := hatReplication.ReplicaPartInventory{
    ReplicaID:  "replica-a",
    Generation: sourceCatalog.Generation(),
    Parts:      sourceParts,
}
target := hatReplication.ReplicaPartInventory{
    ReplicaID:  "replica-b",
    Generation: targetCatalog.Generation(),
    Parts:      targetParts,
}
plan, err := hatReplication.BuildReplicaPartRepairPlan(
    hatReplication.ReplicaPartRepairOptions{}, source, target,
)
if err != nil {
    return err
}
if !plan.IsCurrent(sourceCatalog.Generation(), targetCatalog.Generation()) {
    return errors.New("part snapshots changed; rebuild the plan")
}
for _, action := range plan.Actions {
    switch action.Kind {
    case hatReplication.ReplicaPartRepairCopy:
        // Transfer and verify action.Source, then attach it to the target.
    case hatReplication.ReplicaPartRepairReplace:
        // Stage and verify action.Source before replacing action.Target.
    case hatReplication.ReplicaPartRepairQuarantine:
        // Detach action.Target; do not delete it automatically.
    }
}
```

The source and target entries are copied into the returned plan, including
column metadata, so callers may reuse or mutate their input snapshots after
the call. Part bytes are never copied into the plan.

## Bounds And Safety

- `MaxParts=0` uses the default bound of 65,536 entries per inventory.
- Explicit bounds are limited to 1,048,576 entries.
- Replica and part identifiers are trimmed and bounded to 256 bytes; blank or
  NUL-containing identifiers are rejected.
- Duplicate part names are rejected rather than silently shadowed.
- Input slices are copied and sorted; caller order is not changed.
- The returned actions are sorted by part name and use whole-manifest equality.

The planner assumes the supplied manifests have already been verified by the
part catalog or an equivalent storage-integrity boundary. It compares metadata
only; the transfer path must verify incoming bytes before publication.

## Benchmark

Command:

```text
make benchmark-ch019-replica-repair
```

Machine: AMD Ryzen 9 5950X, Linux amd64, three samples per benchmark, 1,024
equal parts. The baseline is a straightforward two-map diff that does not
provide input validation, deterministic sorting, generation fencing, or copied
metadata safety.

| Implementation | Samples (ns/op) | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| CH-019 planner | 157,439; 154,595; 150,717 | 154,595 | 213,360 | 8 |
| Map baseline | 317,670; 344,576; 304,511 | 317,670 | 524,449 | 10 |

The planner is about 2.1x faster than the reference map diff, uses about 59%
less transient memory, and performs 20% fewer allocations in this equal-manifest
workload. Its sorting and input-copy cost is deliberate: the result is bounded,
deterministic, non-mutating, and safe to inspect before side effects.
