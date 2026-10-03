# T-U38 Conflict Introspection Stream

T-U38 adds an importable, opt-in conflict diagnostic stream inspired by
Tarantool conflict observability. It is intentionally separate from conflict
resolution: callers record a decision only when they need diagnostics, so the
existing resolver and default write path remain unchanged.

## Contract

`hatReplication.ConflictEventLog` provides:

- bounded ring retention by event count and estimated event bytes;
- ordered `ReadAfter` cursor reads with an explicit expired-cursor error;
- keyed 128-bit HMAC-SHA256 key digests instead of retaining raw keys;
- left/right versions, winner, policy, space, and resolved/rejected decision;
- CRC32C-protected `HCE1` binary snapshots and atomic restore;
- caller-owned persistence and transport, with no background goroutine.

The log rejects empty spaces, invalid versions or policies, invalid decisions,
oversized metadata, missing/short hash keys, and snapshots that exceed the
configured bounds. A snapshot contains the digest key fingerprint so restoring
with a different hash key fails closed. CRC32C detects corruption; it is not a
replacement for authenticated storage or transport.

The raw key and values are never stored. Space names and node IDs are retained
as diagnostic metadata, so callers must still avoid putting secrets in those
fields. The caller must protect the snapshot and keep the `HashKey` secret.

## Example

```go
log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{
    Capacity: 1024,
    MaxBytes: 256 << 10,
    HashKey:  secret,
})
event, err := log.Record(space, key, left, right,
    hatReplication.ConflictPolicyLastWriteWins, winner,
    hatReplication.ConflictEventResolved)
page, err := log.ReadAfter(cursor, 128)
snapshot, err := log.MarshalBinary()
```

`ReadAfter` reports a cursor gap through `ErrConflictEventCursorExpired` rather
than silently returning an incomplete history. `MarshalBinary` and
`RestoreBinary` are in-memory operations; filesystem durability, access
control, and publication ordering remain the embedding service's
responsibility.

## Measurement

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | Median ns/op | B/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing conflict resolution, baseline | 2.153 | 0 | 0 | no diagnostic recording |
| Resolution only in feature binary | 2.357 | 0 | 0 | same no-log control; run-to-run noise |
| Resolution plus event record | 262.3 | 8 | 1 | explicit opt-in diagnostic cost |
| Snapshot of 128 events | 5,562 | 14,336 | 1 | 13,853-byte serialized payload |

The optimized record path is 2.28x faster than the initial implementation
(`~588.5 ns`, `544 B`, `8 allocs`) and reduces its allocation footprint by
68x in bytes and 8x in allocation count. The diagnostic path is still much
slower than conflict resolution alone, as expected; it is not enabled
implicitly. The fixed-size ring itself has no process-wide or per-key growth.
Raw samples are in [TR038_BENCHMARK_RAW.txt](TR038_BENCHMARK_RAW.txt).

## Verification

The focused tests cover redaction, bounded eviction, cursor expiry, invalid
configuration, corrupt snapshots, stable restore sequencing, and HMAC digest
compatibility. `go test -race ./hat/hatReplication -run TestT038` and
`go vet ./hat/hatReplication` pass.
