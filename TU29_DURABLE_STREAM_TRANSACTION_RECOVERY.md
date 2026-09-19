# Durable Stream Transaction Recovery

T-U29 adds an opt-in `hatPeer.StreamTransactionRecovery` ledger for the
existing bounded compact peer transaction streams. It records lifecycle state
and SHA-256 fingerprints of stream calls so a caller can reject conflicting
retries and restore committed or pending transaction metadata after a restart.

The ledger does not execute storage operations or automatically reconnect a
`CompactPeerStream`. The stream handler remains responsible for rebuilding its
storage-side transaction and deciding whether a pending transaction is safe to
resume, roll back, or require operator intervention.

## Configuration

```go
recovery, err := hatPeer.OpenStreamTransactionRecovery(
	hatPeer.StreamTransactionRecoveryOptions{
		Path:            "/var/lib/hatrie-cache/peer-transactions.htsr",
		MaxTransactions: 1024,
		MaxOperations:   64,
		MaxPayloadBytes: 1 << 20,
		SyncMode:        hatPeer.StreamTransactionSyncOnCommit,
	},
)
if err != nil {
	return err
}
```

The zero-value limits use the defaults above. The default path is empty, which
keeps the ledger in memory. A configured path writes a CRC32C-protected `HTSR`
snapshot through a private `0600` temporary file, file sync, atomic rename, and
directory sync.

`StreamTransactionSyncOnCommit` is the default. Begin and call fingerprints
stay in memory, while commit and rollback outcomes are durably synced. Call
`Sync()` when pending-state recovery is required before the transaction reaches
a terminal state. `StreamTransactionSyncEveryMutation` syncs every begin, call,
terminal transition, and forget, at higher latency and I/O cost.

## Lifecycle

```go
if err := recovery.Begin(streamID); err != nil {
	return err
}
if err := recovery.Record(streamID, 1, hatPeer.CompactPeerStreamCall, command, payload); err != nil {
	return err
}
if err := recovery.Commit(streamID); err != nil {
	return err
}

var state hatPeer.StreamTransactionSnapshot
if !recovery.LookupInto(streamID, &state) {
	return errors.New("transaction recovery record missing")
}
```

Repeated `Begin`, `Commit`, `Rollback`, and identical sequence fingerprints are
idempotent. Reusing a sequence with a different command or payload is rejected.
`Lookup` returns an isolated copy; `LookupInto` can reuse caller-owned operation
storage for an allocation-free repeated check. `Forget` removes terminal
records so the bounded registry can accept new streams. `Pending` and
`Snapshot` return deterministic stream-ID order.

## Safety

Only command and payload hashes are stored; raw request data is not written to
the recovery file. CRC32C detects accidental truncation or corruption but is
not authentication. Keep the file on a protected local filesystem and use an
authenticated storage or encryption layer when an attacker can modify it.
The bounds prevent unbounded transaction, operation, payload, and snapshot
memory. The feature has no effect on existing stream endpoints until the
caller explicitly wires it into a handler.

## Measurement

Five samples on the development host, with a one-operation pending record:

| Workload | Median CPU | Memory | Relative cost |
| --- | ---: | ---: | ---: |
| Defensive `Lookup` copy | 52.45 ns/op | 80 B/op, 1 alloc/op | 17.8x direct-map control |
| Buffered `LookupInto` | 15.05 ns/op | 0 B/op, 0 allocs/op | 5.1x direct-map control |
| Direct-map control | 2.95 ns/op | 0 B/op, 0 allocs/op | control |
| Default persisted transaction (`sync-on-commit`) | 1.66 ms/op | 3,224 B/op, 44 allocs/op | durability path |

The first implementation synced the whole snapshot on every mutation and
measured 6.73 ms/op, 6,740 B/op, and 95 allocations. The default sync-on-commit
policy is therefore about 4.06x faster, 2.09x lower in measured bytes, and
2.16x fewer allocations for the same four-step benchmark. This is not a
default request-path optimization: it is an explicit durability cost for
callers that enable persistent recovery.

Reproduce with `make verify-tu29` or `make benchmark-tu29` in the repository
worktree.

The clean remote full-package baseline also contains unrelated existing test
issues (`TestCompactProtocolRoundTrip` expects a different frame size and a TLS
test does not terminate). The affected stream-only baseline passes; the
recovery tests, stream regressions, race checks, and vet pass independently.
