# T-U38: Conflict Introspection Stream

`hatReplication.ConflictEventLog` is an opt-in bounded stream for conflict
diagnostics. It records the space, left/right sources, selected decision, and a
cursor sequence. The user key is never retained: the log stores only a
SHA-256 digest of the configured salt plus key.

## Record And Read

Use `ResolveAndRecord` when the policy result should be captured together with
the conflict decision:

```go
log, _ := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{
    Capacity: 1024,
    HashSalt: []byte("operator-supplied-non-secret-salt"),
})
winner, err := registry.ResolveAndRecord(log, "orders", key, left, right)
```

Rejected conflicts are recorded before `ErrConflictRejected` is returned.
Consumers page through the bounded history with `Read(after, limit)` and pass
`ConflictEventPage.NextSequence` as the next cursor. A cursor older than the
retained ring returns `ErrConflictEventHistoryGap`, allowing a consumer to
request a fresh diagnostic snapshot instead of silently skipping events.

`Append` is available when a caller already performed resolution. `Len` and
the page bounds expose retained-history size without exposing row values.

## Durable Snapshot

`MarshalBinary` emits a deterministic CRC32C-protected `cel1` snapshot;
`UnmarshalConflictEventLog` validates the version, bounds, sequence continuity,
and checksum before allocating the restored ring. The caller owns durable file
placement, atomic replacement, permissions, and backup retention. Snapshots
include the configured hash salt so restored digests remain stable. The salt is
not encryption; use a secret-management boundary when key pseudonym resistance
against an operator with the snapshot itself is required.

The ring is bounded by `MaxConflictEventLogCapacity` and reads are bounded by
`MaxConflictEventReadLimit`. Invalid or oversized input is rejected before
unbounded decode allocation.

## Defaults And Tradeoff

No log is created or updated by default, so ordinary conflict resolution keeps
its existing behavior and cost. Enabling `ResolveAndRecord` intentionally adds
key hashing, a bounded ring write, and event materialization. This is an
operator-facing diagnostic path, not a replacement for the hot resolver.

Measured on Linux/amd64 with an AMD Ryzen 9 5950X:

| Workload | Result |
| --- | ---: |
| Existing resolver control | 2.68 ns/op, 0 B/op, 0 allocs/op |
| Existing resolver in feature build | 2.90 ns/op, 0 B/op, 0 allocs/op |
| Opt-in resolve plus redacted record | 275 ns/op, 160 B/op, 3 allocs/op |
| Snapshot of 1,024 retained events | 68,648 bytes; 63,598 ns/op; 139,318 B/op; 1 alloc/op |

The small control-path difference is benchmark noise; the diagnostic-path cost
is explicit and scales with the configured event capacity only for retained
memory, not for ordinary resolution.
