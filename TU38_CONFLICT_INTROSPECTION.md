# T-U38 Conflict Introspection Stream

`hatReplication.ConflictEventLog` is an opt-in bounded stream for conflict
diagnostics. It records the named space, SHA-256 key digest, writer versions,
winner, and policy decision without retaining values or plaintext keys.

The log is useful when an operator needs to answer which peers conflicted and
which policy won, while keeping the conflict payload out of diagnostic data.
The caller owns the integration point: conflict resolution code must call
`Append` after it has selected or rejected a winner.

## Example

```go
log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{
    Capacity: 4096,
})
if err != nil {
    return err
}

event, err := log.Append(hatReplication.ConflictEventInput{
    Space: "orders",
    Key: []byte("order:42"),
    Left: hatReplication.ConflictVersion{
        Timestamp: 100,
        NodeID: "region-a",
        Sequence: 17,
    },
    Right: hatReplication.ConflictVersion{
        Timestamp: 101,
        NodeID: "region-b",
        Sequence: 9,
    },
    Winner: hatReplication.ConflictVersion{
        Timestamp: 101,
        NodeID: "region-b",
        Sequence: 9,
    },
    Decision: hatReplication.ConflictDecisionLastWriteWins,
})
if err != nil {
    return err
}

batch := log.Read(0, 100)
// batch.Events[0].KeyDigest identifies order:42 without exposing it.
_ = event
_ = batch
```

`Read(afterID, limit)` returns detached events in ID order. Pass the returned
`NextID` to continue. When the requested cursor has fallen out of the bounded
retention window, `HistoryGap` is true and the caller should restart from the
oldest retained ID. `Wait` provides the same cursor behavior with context
cancellation and does not start a goroutine per waiter.

## Persistence And Recovery

`MarshalBinary` writes a deterministic HCI1 snapshot containing the retained
events, capacity, and next ID. The payload is CRC32C checked and restores with:

```go
wire, err := log.MarshalBinary()
if err != nil {
    return err
}
restored, err := hatReplication.NewConflictEventLogFromBinary(wire)
if err != nil {
    return err
}
```

The API does not write files or perform fsync. Persist `wire` through the
application's existing atomic, mode-restricted storage path. CRC32C detects
accidental corruption; it is not an authenticity or confidentiality boundary.
Use authenticated/encrypted storage when snapshots cross a trust boundary.

## Limits And Security

- The default capacity is 1,024 events; the maximum is 65,536.
- The default read limit is 256 events; the maximum is 4,096.
- Space and source identifiers are capped at 256 bytes.
- The key is hashed immediately with SHA-256 and is never retained by the log.
- A bare SHA-256 digest can still be dictionary-tested for predictable keys; do
  not treat `KeyDigest` as a secret.
- Values, command payloads, and arbitrary user metadata are not accepted.
- Retention is a fixed ring, so appends evict the oldest events predictably.
- Constructing a log does not alter conflict resolution and does not enable
  automatic replication, HTTP endpoints, or persistence.

## Measured Cost

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Operation | Median CPU | Heap/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Append | 283.2 ns | 240 B | 3 |
| Read 64 events | 2,489 ns | 14,336 B | 1 |
| Marshal 1,024 retained events | 227,286 ns | 655,360 B | 1,027 |

Append cost is fixed by event metadata and ring capacity. Read cost is
proportional to the requested detached batch, and snapshot cost is proportional
to retained history because it copies and checksums the durable representation.
The benchmark is reproducible with `make benchmark-round40-conflict` and the
raw results are recorded in [BENCHMARK.md](BENCHMARK.md#t-u38-conflict-introspection-stream).
