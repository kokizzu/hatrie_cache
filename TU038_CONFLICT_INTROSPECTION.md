# T-U38 Conflict Introspection Stream

`hatReplication.ConflictIntrospectionLog` is an opt-in, bounded stream for
diagnosing replication conflict decisions. It records the space, source
versions, policy mode, winner/rejection decision, and a keyed digest of the
application key without retaining the raw key.

## Usage

```go
log, err := hatReplication.NewConflictIntrospectionLog(
    hatReplication.ConflictIntrospectionOptions{
        MaxEvents:     4096,
        KeyHashSecret: conflictDigestSecret,
    },
)
if err != nil {
    return err
}

winner, err := registry.ResolveWithKey(
    log,
    "payments",
    accountKey,
    leftVersion,
    rightVersion,
)
_ = winner
_ = err

page, err := log.ReadSince(cursor, 256)
if err != nil {
    return err
}
for _, event := range page.Events {
    fmt.Println(event.Sequence, event.Space, event.KeyHashHex(), event.Decision)
}
cursor = page.NextSequence - 1
```

`Resolve` remains the unchanged zero-overhead path. `ResolveWithKey` only
records an event when a log is supplied, and logging failures never replace the
underlying resolution result.

## Retention And Recovery

The log is a fixed-size ring. `ReadSince` returns `Gap=true` when the requested
cursor predates retained events, allowing a consumer to request a fresh
checkpoint or snapshot. The default capacity is 1,024 events; `MaxEvents` may
be set up to 65,536. Read pages default to 256 events and are capped at 4,096.

`MarshalBinary` and `UnmarshalBinary` provide deterministic `HCIS1` snapshots
with CRC32C validation. Snapshots contain no secret and no raw key. The caller
owns durable storage and should configure a new log with the same high-entropy
secret when it needs key-digest correlation after restore.

## Security

Key identifiers are the first 128 bits of HMAC-SHA256 using the required
`KeyHashSecret`, which prevents offline guessing from the digest alone. The
secret must be at least 16 bytes and should be generated from high-entropy
secret material; it is never serialized. Space names and source node IDs are
retained for diagnostics, so callers should avoid putting sensitive values in
those fields.
Malformed snapshots, invalid versions, invalid policy values, and checksum
failures are rejected without replacing the current stream.

## Measured Tradeoff

Five benchmark samples on an AMD Ryzen 9 5950X, Linux amd64, with a single
conflict key and a 4,096-event ring:

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing `Resolve` | 15.44 | 0 | 0 | baseline |
| `ResolveWithKey` + log | 265.8 | 40 | 2 | 17.2x slower |

This is diagnostic overhead, not a claim of faster conflict resolution. It is
paid only by callers that explicitly construct and pass a log; existing
resolution and replication defaults retain the baseline path.

Run the focused checks with:

```text
make test-tu038-conflict-introspection
make benchmark-tu038-conflict-introspection
```
