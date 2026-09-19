# Per-Space WAL Sync Policy

T-U34 adds an opt-in, bounded policy registry in `hatJournal` for callers that
need different durability policies for named spaces. It is a policy resolver;
it does not silently change the existing journal writer or the default
configuration.

## Policies

| Policy | Meaning | Durability tradeoff |
| --- | --- | --- |
| `periodic` | Use the journal's normal group-commit and periodic sync behavior. | The default balanced choice; recent writes can wait for the next sync or be lost on an abrupt failure. |
| `immediate` | Sync each committed append before reporting it durable. | Stronger crash durability with more sync calls and lower write throughput. |
| `disabled` | Do not require a WAL sync for the space. | Lowest write latency, but acknowledged data can be lost after a process or host failure. |

The registry defaults to `periodic`, and the feature is otherwise disabled until
a caller constructs a registry and consults it. A missing space override falls
back to the configured default.

## Example

```go
policies, err := hatJournal.NewSpaceSyncPolicyRegistry(hatJournal.SpaceSyncPolicyOptions{
	Capacity:      128,
	DefaultPolicy: hatJournal.SpaceSyncPolicyPeriodic,
})
if err != nil {
	return err
}
if err := policies.Register("orders", hatJournal.SpaceSyncPolicyImmediate); err != nil {
	return err
}

policy := policies.Resolve("orders")
// The journal owner applies policy before/after its append:
// periodic  -> normal group commit
// immediate -> append, sync, then acknowledge
// disabled  -> append without a required WAL sync
_ = policy
```

Space names are trimmed and must be non-empty and at most 256 bytes. Registry
capacity defaults to 128 entries and is bounded at 4,096 entries. Re-registering
a space replaces its policy without consuming another slot. `Snapshot` returns a
deterministically sorted copy suitable for diagnostics or configuration
snapshots.

## Integration Boundary

The registry deliberately does not own append, fsync, recovery, authentication,
or replication. The concrete journal/space owner must resolve the policy at its
write boundary and keep its crash-recovery contract consistent with that choice.
This keeps the current `CommandJournal` behavior unchanged and avoids creating
a false durability guarantee for callers that have not wired the policy into a
writer.

Treat `disabled` as an explicit risk decision. It should normally be restricted
to rebuildable or best-effort data, and its use should be visible in operational
configuration and backup/recovery documentation.

## Measurement

The registry lookup benchmark uses a warmed two-entry registry and compares it
with a direct two-entry Go map lookup. Five samples on the development host:

| Lookup | Median CPU | Allocations |
| --- | ---: | ---: |
| Registry `Resolve("orders")` | 12.89 ns/op | 0 B/op, 0 allocs/op |
| Direct map lookup control | 7.15 ns/op | 0 B/op, 0 allocs/op |

The registry costs about 1.80x the raw-map lookup in this control benchmark,
while retaining bounded capacity, normalization, default fallback, replacement,
and concurrency-safe snapshots. It is intended for the per-write policy check,
not as a faster map implementation. Reproduce with:

```sh
make benchmark-tu34
make verify-tu34
```

The existing `hatCache` benchmark baseline could not be compiled at the clean
remote base because that checkout has unrelated missing SQL symbols
(`MaxDataflowTextBytes`, `TypedTableDate`, and `TypedTableTimestamp`). No
end-to-end journal throughput improvement is claimed from this registry alone.
