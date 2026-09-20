# Per-Space WAL Sync Policy

T-U34 adds an opt-in, bounded policy registry in `hatJournal` and wires it into
`hatCache.CommandJournal`. Callers can choose different durability policies for
named spaces without changing the existing journal format or legacy command
path. A nil registry preserves the periodic default.

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
policies, err := hatCache.NewSpaceSyncPolicyRegistry(hatCache.SpaceSyncPolicyOptions{
	Capacity:      128,
	DefaultPolicy: hatCache.SpaceSyncPolicyPeriodic,
})
if err != nil {
	return err
}
if err := policies.Register("orders", hatCache.SpaceSyncPolicyImmediate); err != nil {
	return err
}

journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	SpaceSyncPolicies: policies,
})
if err != nil {
	return err
}
defer journal.Close()

response := journal.ExecuteCommandInSpace(trie, "orders", request)
_ = response
```

The existing `ExecuteCommand(trie, request)` method remains equivalent to an
unnamed periodic command. `ExecuteCommandInSpace` applies the resolved policy
at the append boundary; the logical space name is metadata only and is not
serialized into journal records, so recovery remains compatible with existing
files.

Space names are trimmed and must be non-empty and at most 256 bytes. Registry
capacity defaults to 128 entries and is bounded at 4,096 entries. Re-registering
a space replaces its policy without consuming another slot. `Snapshot` returns a
deterministically sorted copy suitable for diagnostics or configuration
snapshots.

## Journal Integration

`CommandJournalOptions.SpaceSyncPolicies` accepts the registry. Direct appends
resolve the policy before writing. Group-commit jobs carry their resolved policy
and are split around `immediate` jobs so each immediate command is synced before
its response is acknowledged. Periodic and disabled jobs can still share a
batch; the batch syncs when any member requires durability. Idempotent group
commit follows the same rule.

The registry deliberately does not own authentication, replication, recovery,
or space identity. The caller must use one canonical name for each logical
space and must keep disabled spaces rebuildable or otherwise acceptable to lose.
Rejected writes may still sync while the journal safely rolls back their record;
`disabled` means successful appends do not require an explicit WAL sync.

Treat `disabled` as an explicit risk decision. It should normally be restricted
to rebuildable or best-effort data, and its use should be visible in operational
configuration and backup/recovery documentation.

## Measurement

The registry lookup benchmark uses a warmed two-entry registry and compares it
with a direct two-entry Go map lookup. Five samples on the development host
measured `12.89 ns/op` versus `7.15 ns/op`, with `0 B/op` and `0 allocs/op` in
both cases. The registry costs about `1.80x` the raw-map lookup while adding
bounded admission, normalization, validation, fallback, replacement, and
concurrent snapshots.

The journal benchmark uses a single-command `SETSTR` workload with a no-op sync
hook to isolate policy dispatch from device-specific fsync latency. The
post-change common path retained `256 B/op` and `2 allocs/op`, with a median
`4.08 us/op` versus `4.34 us/op` at `origin/master` (`1.06x`, about 5.9%
lower CPU time). Periodic and disabled named-space paths measured `4.19 us/op`
and `4.18 us/op`, respectively. These are small microbenchmark differences,
not a claim that disabled durability is free on real storage.

Reproduce the focused checks and measurements with:

```sh
make test-tu34
make test-tu34-journal
make race-tu34
make vet-tu34
make benchmark-tu34-baseline
make benchmark-tu34-space-sync
make verify-tu34
```

The clean remote baseline required a temporary test-only compatibility shim for
unrelated missing SQL symbols (`MaxDataflowTextBytes`, `TypedTableDate`, and
`TypedTableTimestamp`); that shim is not part of this feature.
