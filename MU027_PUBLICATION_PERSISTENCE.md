# M-U27 Durable Logical Publication History

`hatSql.SQLPublication` already provides bounded contiguous revisions and
consumer checkpoints. This follow-up adds an opt-in binary snapshot of the
retained publication history so a process restart does not force every
consumer to resynchronize from the source.

## API

```go
encoded, err := publication.MarshalBinary()
if err != nil {
    return err
}

restored, err := hatSql.UnmarshalSQLPublication(encoded)
if err != nil {
    return err
}
```

The snapshot contains the publication name, fixed schema, normalized bounds,
retained batches, latest frontier, and closed state. Subscriber channels and
consumer checkpoints are intentionally excluded. Restore returns a new
publication and publishes nothing until the caller installs it.

## Format And Safety

- The versioned `HPS1` frame is length-delimited and protected by CRC32 over
  its header and payload.
- `MaxSQLPublicationSnapshotBytes` is 64 MiB. Counts, strings, nested values,
  and recursion are bounded before allocation.
- SQL scalar values retain their exact supported Go scalar type, including
  integers, floats, booleans, strings, bytes, times, dates, decimals, UUIDs,
  durations, and IP values.
- `map[string]interface{}` and `[]interface{}` values are recursively
  preserved with deterministic map-key ordering. Unsupported row values fail
  with `ErrSQLPublicationInvalid`; they are never silently converted.
- CRC detects corruption and truncation. It is not authentication or
  encryption. Untrusted snapshots still require transport authentication and,
  where appropriate, encryption and authorization.

Write snapshots to a caller-owned temporary file, `fsync`, atomically rename,
and apply the caller's permission and key-management policy. Persist a
consumer checkpoint only after its sink transaction is durable.

## Recovery

1. Read and validate the latest snapshot before accepting new publication
   batches.
2. Restore each consumer's separately persisted checkpoint.
3. Subscribe from that checkpoint. Retained batches replay in revision order;
   an expired checkpoint still returns `ErrSQLPublicationCheckpointExpired`.
4. Resume source/connector delivery only after the restored publication is
   installed.

The snapshot is not a source-table backup and does not include connector
credentials, source offsets outside publication revisions, or sink effects.

## Measurement

On the repository's AMD Ryzen 9 5950X benchmark host, five samples over 128
one-delta batches produced:

| Operation | Time | Allocated | Snapshot |
| --- | ---: | ---: | ---: |
| Marshal | 176.7-183.2 us/op | 118,073-118,081 B/op, 2,953 allocs/op | 12,165 B |
| Unmarshal | 265.7-269.4 us/op | 217,299-217,300 B/op, 5,009 allocs/op | 12,165 B |

The existing publication hot path remained unchanged in the same before/after
run: direct send stayed at roughly 30 ns/op with zero allocation, append at
roughly 430 ns/op and 392 B/op, and replay/ack at roughly 22.7 us/op and
29,760 B/op. Persistence is therefore a restart/recovery capability, not a
hot-path optimization.
