# T-U11 Conflict Policy Snapshots

`hatReplication.ConflictPolicyRegistry` already supports bounded per-space
last-write-wins, source-priority, and reject policies. This addition exposes a
point-in-time `ConflictPolicySnapshot` for operators and caller-owned write
paths that need to explain which policy was active when a conflict was
resolved.

```go
registry, err := hatReplication.NewConflictPolicyRegistry(
	hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyLastWriteWins},
)
if err != nil {
	panic(err)
}

if err := registry.Set("payments", hatReplication.ConflictPolicy{
	Mode:           hatReplication.ConflictPolicySourcePriority,
	SourcePriority: []string{"region-a", "region-b"},
}); err != nil {
	panic(err)
}

snapshot, err := registry.Snapshot("payments")
if err != nil {
	panic(err)
}
// snapshot.Generation can be stored with a caller-owned conflict decision.
winner, err := registry.Resolve("payments", local, remote)
```

## Semantics

- `Space` is the normalized requested space name.
- `Policy` is the effective default or per-space override.
- `Overridden` identifies whether a per-space override was selected.
- `Generation` starts at zero and increases after every successful `Set` or
  successful `Delete` of a per-space override.
- Invalid requests and deleting a missing override do not advance the
  generation.
- The returned policy and source-priority slice are copies. Mutating a
  snapshot cannot change later resolution.
- `Resolve` remains the compatibility path and still performs no snapshot
  allocation. The registry does not automatically apply the decision to a
  cache, journal, or replication transport; callers still own that boundary.

The snapshot is intended for audit records, migration checks, and control-plane
diagnostics. A caller can record the space, generation, policy mode, input
versions, and result without exposing the registry's mutable configuration.

## Performance Decision

The existing resolver was measured before the change. An attempted immutable
atomic copy-on-write implementation was also measured, but it did not improve
the hot path and made priority resolution slower, so it was removed. The
shipped implementation retains the existing `RWMutex` resolver path and adds
only the snapshot API and generation bookkeeping. See the raw data in
[BENCHMARK.md](BENCHMARK.md#t-u11-conflict-policy-snapshots).

The snapshot API has an intentional cost: a default-policy snapshot measured
about 19 ns/op with zero allocations, while copying a two-source priority
policy measured about 46 ns/op, 32 B/op, and one allocation. Use `Resolve` in
the data path and `Snapshot` only when the policy metadata is needed.
