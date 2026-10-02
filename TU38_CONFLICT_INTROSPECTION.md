# T-U38 Conflict Introspection

`hat/hatReplication` now provides an opt-in bounded history for replication conflict decisions. The design follows Tarantool's operational visibility around replication conflicts and Materialize's bounded, cursor-oriented diagnostic streams without changing conflict resolution or write behavior.

## What it records

`ConflictIntrospectionLog.Record` accepts the named space, both competing `ConflictVersion` values, the winner or rejection outcome, and the key. The key is hashed immediately with SHA-256 and only the first 16 bytes, encoded as 32 lowercase hexadecimal characters, are retained. Raw keys and values are never stored in the record.

Each record has a monotone cursor, timestamp, source versions, outcome, and redacted key digest. `Read(after, limit)` supports bounded replay. When the ring has evicted the requested cursor, it returns `ErrConflictIntrospectionCursorExpired` instead of silently skipping history.

The zero-value path is unchanged: callers must construct a log explicitly. The default capacity is 1,024 records and the hard maximum is 65,536. A caller can persist `MarshalBinary()` output and restore it with `UnmarshalConflictIntrospectionLog`; the `CIR1` snapshot is deterministic and CRC32C checked. Persistence, transport, access control, and retention beyond the bounded ring remain caller-owned.

## Example

```go
log, err := hatReplication.NewConflictIntrospectionLog(
	hatReplication.ConflictIntrospectionLogOptions{Capacity: 4096},
)
if err != nil {
	return err
}

_, err = log.Record(hatReplication.ConflictIntrospectionInput{
	Space:   "orders",
	Key:     []byte("order:42"),
	Left:    leftVersion,
	Right:   rightVersion,
	Winner:  winner,
	Outcome: hatReplication.ConflictIntrospectionRightWon,
})
if err != nil {
	return err
}

records, cursor, err := log.Read(0, 100)
```

The API is deliberately separate from `ConflictPolicyRegistry.Resolve`: an application can record only conflicts it is authorized to expose and can choose the durable destination appropriate for its deployment.

## Benchmark

Measured on Linux/amd64 with an AMD Ryzen 9 5950X using `-benchtime=100ms -count=1`:

| Operation | ns/op | B/op | allocs/op | Meaning |
| --- | ---: | ---: | ---: | --- |
| Existing `ResolveConflictVersion` | 6.526 | 0 | 0 | Conflict decision without introspection |
| `ConflictIntrospectionLog.Record` | 150.6 | 32 | 1 | Opt-in record, including key digest |
| `ConflictIntrospectionLog.Read` (32 records) | 942.3 | 5376 | 1 | One bounded cursor page |

The log is not on the existing resolution path unless a caller explicitly invokes `Record`. This makes the default cost zero. When enabled, the measured recording overhead is the digest, one string allocation, synchronization, and ring insertion; the tradeoff is bounded diagnostic history rather than lower write latency.

## Security and privacy

The digest prevents accidental raw-key disclosure in exported records but is not encryption. Low-entropy keys can be dictionary-attacked. Protect snapshots and read endpoints with the same authorization as replication metadata, and do not treat `KeyDigest` as a secret or an access token.

## Verification

The focused tests cover redaction, deterministic snapshot round trips, CRC rejection, ring eviction, expired cursors, invalid input, and zero-value safety. The Makefile targets also run race, vet, full package, and benchmark checks.
