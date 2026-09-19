# T-U38 Conflict Introspection Stream

`hatReplication.ConflictIntrospectionLog` is an opt-in, bounded event log for
replication conflict diagnostics. It retains conflict metadata only:

- space name;
- caller-supplied 32-byte key digest;
- both conflict versions and their node IDs;
- a decision (`left wins`, `right wins`, `rejected`, or `equal`); and
- an optional event timestamp.

Raw keys and values are not accepted by the event type. Use a keyed digest such
as HMAC-SHA-256 when a key could be guessed from an unsalted hash.

## Usage

```go
log, err := hatReplication.NewConflictIntrospectionLog(
    hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 4096},
)
if err != nil {
    return err
}

event, err := log.Record(hatReplication.ConflictIntrospectionEvent{
    Space:     "orders",
    KeyDigest: digest,
    Left:      leftVersion,
    Right:     rightVersion,
    Decision:  hatReplication.ConflictIntrospectionRightWins,
})
if err != nil {
    return err
}
_ = event.Sequence

events, err := log.Replay(lastConsumedSequence, 512)
if errors.Is(err, hatReplication.ErrConflictIntrospectionHistoryGap) {
    // The consumer must rebuild its diagnostic view from a fresh snapshot.
}
```

`Record` assigns monotone sequences. Once the configured ring is full, the
oldest event is evicted. `Replay` refuses to return a partial history and
reports `ErrConflictIntrospectionHistoryGap` when the requested sequence is no
longer retained. A limit of zero means the bounded maximum replay size.

## Durable Snapshot

`MarshalBinary` produces a bounded `CIF1` binary snapshot with a CRC32C
checksum. `DecodeConflictIntrospectionLog` and `UnmarshalBinary` validate the
magic, version, lengths, contiguous sequences, event fields, and checksum
before publishing any decoded state. A failed restore leaves the existing log
unchanged. The caller owns the durable file/object-store write, encryption,
retention, and authorization policy.

The log does not install a background worker, network listener, or global
registry. Existing conflict resolution remains unchanged and zero-allocation;
the caller explicitly records an event only at the conflict boundary it wants
to observe.

## Measurement

AMD Ryzen 9 5950X, Linux/amd64, five `-benchmem` samples. The stream fixture
retains 256 events.

| Operation | Existing/direct or JSON baseline | Binary log | Result |
| --- | ---: | ---: | ---: |
| Direct conflict resolution | 6.57 ns/op, 0 B/op, 0 allocs/op | 85.79 ns/op for explicit `Record` | 13.1x opt-in overhead |
| Snapshot encode | JSON: 121,176 ns/op, 68,905 wire bytes | CIF1: 19,057 ns/op, 27,925 wire bytes | 6.36x faster, 2.47x smaller |
| Snapshot decode | JSON: 869,389 ns/op, 88,539 B/op, 1,041 allocs/op | CIF1: 41,307 ns/op, 47,109 B/op, 769 allocs/op | 21.0x faster, 26% fewer allocs |

The recording overhead is a diagnostic cost, not a change to the default
conflict path. Snapshot restore allocates a detached event slice so corruption
cannot partially mutate the live log.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#t-u38-conflict-introspection-stream).
