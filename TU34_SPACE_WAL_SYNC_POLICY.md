# Per-Space WAL Sync Policy

T-U34 provides an opt-in, bounded policy registry and a concrete
`hatJournal.SpaceSyncAppender` boundary for callers that need different WAL
durability policies for named spaces. Existing journal writers and all default
configuration remain unchanged until a caller opts into the boundary.

## Policies

| Policy | Meaning | Durability tradeoff |
| --- | --- | --- |
| `periodic` | Append normally and sync when the bounded byte or time threshold is reached, or when `Flush` is called. | Balanced write cost; recent writes can remain unsynced until the next boundary. |
| `immediate` | Sync each successful append before returning a successful durable result. | Stronger crash durability with more sync calls and lower write throughput. |
| `disabled` | Append without a required sync. | Lowest write latency, but acknowledged data can be lost after a process or host failure. |

The registry defaults to `periodic`, and the feature is otherwise disabled until
a caller constructs an appender. A missing space override falls back to the
configured default. The appender's default periodic thresholds are 2 ms and
64 KiB; both are configurable and bounded.

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

appender, err := hatJournal.NewSpaceSyncAppender(file, file.Sync, hatJournal.SpaceSyncAppenderOptions{
	Registry: policies,
})
if err != nil {
	return err
}
result, err := appender.Append("orders", encodedRecord)
if err != nil {
	return err
}
if !result.Synced {
	// Periodic writes are pending; call Flush at the caller's durability boundary.
	if _, err := appender.Flush(); err != nil {
		return err
	}
}
```

`Append` serializes the writer and sync function so a sync cannot overtake its
write. `SpaceSyncAppendResult.Synced` reports whether the append was followed
by a successful sync. A failed sync leaves the required bytes pending so a
later `Flush` can retry. Disabled writes are not required to become durable,
although a later flush may include bytes already pending from another space.

Space names are trimmed and must be non-empty and at most 256 bytes. Registry
capacity defaults to 128 entries and is bounded at 4,096 entries. Re-registering
a space replaces its policy without consuming another slot. `Snapshot` returns
a deterministically sorted copy suitable for diagnostics or configuration
snapshots.

## Integration Boundary

The appender owns policy lookup, write/sync ordering, periodic thresholds, and
retryable pending state. The caller still owns record encoding, recovery,
authentication, replication, and the durable storage lifecycle. The existing
`hatCache.CommandJournal` remains unchanged because its public command request
does not carry a logical space; callers with named spaces can use this helper at
their concrete append boundary without creating a false default durability
guarantee.

Treat `disabled` as an explicit risk decision. Restrict it to rebuildable or
best-effort data and make the choice visible in operational configuration and
backup/recovery documentation.

## Measurement

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. All paths measured
zero allocations. The direct writer is a control, not a production WAL writer.

| Workload | Median ns/op | Syncs/op | Relative CPU |
| --- | ---: | ---: | ---: |
| Direct writer control | 0.249 | 0 | control |
| Appender, `disabled` | 14.69 | 0 | 59.0x control |
| Appender, `periodic` pending | 62.38 | 0 | 250x control |
| Appender, `immediate` | 57.58 | 1 | 231x control |

The overhead is the intentional cost of policy lookup, serialization, and
durability bookkeeping. It is opt-in; the existing journal path pays none of
this cost. The registry-only baseline was 11.86 ns/op for a normalized lookup
and 6.809 ns/op for a direct-map control, both with zero allocations.

Reproduce with:

```sh
make benchmark-tu34
make verify-tu34
```

The benchmark script uses a temporary file with a cleanup trap and leaves no
T-U34 artifacts in `/tmp`.
