# T-U38 Conflict Introspection Stream

T-U38 adds an opt-in, bounded redacted stream for replication conflict
diagnostics. It records which versions conflicted, which policy decision was
made, and a cursor that operators can use to resume inspection without
retaining application keys or values.

## Usage

```go
secret := []byte("a deployment-owned secret of at least 16 bytes")
digest, err := hatReplication.RedactConflictKey(secret, key)
if err != nil {
	return err
}

log, err := hatReplication.NewConflictIntrospectionLog(
	hatReplication.ConflictIntrospectionOptions{Capacity: 2048},
)
if err != nil {
	return err
}

winner, err := hatReplication.ResolveConflictWithIntrospection(
	policyRegistry, log, "accounts", digest, leftVersion, rightVersion,
)
```

`ResolveConflictWithIntrospection` is an explicit wrapper. The existing
`ResolveConflictVersion` and `ConflictPolicyRegistry.Resolve` paths do not
allocate, retain records, or change behavior. Equal versions are not recorded.
Applied conflicts record the left or right winner; reject-policy conflicts are
recorded before `ErrConflictRejected` is returned.

## Replay And Recovery

`Replay(after, limit)` returns ascending sequence numbers. Cursor `0` starts at
the oldest retained event. A nonzero cursor older than the bounded window
returns `ErrConflictIntrospectionHistoryGap` instead of silently skipping
events. The ring defaults to 1,024 records and is bounded at 65,536 records.

`MarshalBinary` and `RestoreBinary` use the deterministic, CRC32C-protected
CIS1 snapshot format. Restore validates all lengths, UTF-8/control characters,
version ordering, decisions, and checksums before atomically publishing the
new ring. Snapshots are bounded to 16 MiB. The caller decides where to persist
the snapshot and when to rotate it; no background writer or network endpoint
is started automatically.

## Redaction And Security

Only a 32-byte HMAC-SHA256 hexadecimal digest is accepted as `KeyDigest`.
`RedactConflictKey` requires a caller-owned secret of at least 16 bytes and
does not retain either input. Records contain space names, node/version
metadata, decisions, and digests, but never raw keys or values. Space and node
identifiers reject invalid UTF-8, control characters, and oversized input.
Operators should still treat digests and node names as sensitive metadata and
protect the snapshot with the deployment's normal access controls.

## Measured Tradeoff

On the paired five-run benchmark in `BENCHMARK.md`, direct winner selection
remained 6.367 ns/op, 0 B/op, and 0 allocs/op. The explicit introspection
wrapper measured 301.4 ns/op, 96 B/op, and 3 allocs/op: about 47.3x CPU, with
96 additional bytes and 3 allocations per recorded conflict. This cost is
opt-in and applies only when a caller requests diagnostic recording; the
ordinary conflict path is unchanged.

Verification:

```text
make test-tu38-red
make test-tu38
make race-tu38
make vet-tu38
make benchmark-tu38
```
