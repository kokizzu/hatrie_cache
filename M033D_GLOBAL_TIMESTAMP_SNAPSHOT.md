# Durable Global Timestamp Oracle Snapshots

M033d adds an opt-in binary snapshot codec and atomic file store for
`hatReplication.GlobalTimestampOracleSnapshot`. The existing JSON form remains
available for diagnostics and compatibility. The new form is intended for
coordinator state transfer and restart recovery where bounded input, compact
storage, and deterministic bytes matter.

## API

```go
store, err := hatReplication.NewGlobalTimestampOracleSnapshotFileStore(
    "/var/lib/hatrie-cache/timestamp-oracle.snapshot",
)
if err != nil {
    return err
}

if err := store.Save(snapshot); err != nil {
    return err
}

restored, err := store.Load()
if err != nil {
    return err
}
```

The codec can be used without a file:

```go
encoded, err := hatReplication.MarshalGlobalTimestampOracleSnapshotBinary(snapshot)
if err != nil {
    return err
}
restored, err := hatReplication.UnmarshalGlobalTimestampOracleSnapshotBinary(encoded)
```

`ErrGlobalTimestampOracleSnapshotInvalid` covers malformed, corrupt, oversized,
or semantically inconsistent snapshots. `MaxGlobalTimestampOracleSnapshotBytes`
is `1 MiB`; the decoder reads at most that limit plus one byte before rejecting
an oversized file. The format also limits the node count to 65,535 and each
NodeID to 4,096 bytes.

## Binary Format

The frame is deterministic and consists of:

1. Four-byte ASCII magic `GTO1`.
2. One-byte format version, currently `1`.
3. Unsigned varints for oracle term, current timestamp, and node count.
4. One record per node, sorted by NodeID. Each record contains length-prefixed
   NodeID text, node epoch, sequence, observed timestamp, grant term,
   length-prefixed grant NodeID, grant epoch, grant sequence, grant start,
   grant end, and grant count.
5. A four-byte little-endian CRC32C over the preceding bytes.

Timestamps are validated as non-negative before being encoded. Marshal accepts
an unsorted input slice, canonicalizes it by NodeID, and rejects duplicate
NodeIDs. Unmarshal requires canonical ordering, checks that the entire payload
was consumed, verifies the checksum, and validates grant/node invariants before
returning the snapshot.

CRC32C detects accidental corruption and torn or stale files; it is not a
cryptographic signature or an authentication mechanism. Untrusted network
traffic still needs the authenticated transport or an external integrity and
authentication layer.

## File Store Guarantees

- `Save` validates and encodes before touching the existing file.
- The parent directory is created with mode `0750`.
- Temporary files are private (`0600`), flushed, and atomically renamed.
- The containing directory is synced after publication.
- A failed validation, write, or encode leaves the previous snapshot intact.
- `Load` validates the complete frame before returning any state.

The file store persists a caller-selected snapshot. It does not decide when a
consensus transition is committed, elect a leader, or publish a grant. Those
coordination decisions remain with the caller.

## Benchmark

Command:

```text
make benchmark-m033d-global-timestamp-snapshot
```

Host: Linux/amd64, AMD Ryzen 9 5950X. Fixture: two nodes, five benchmark
samples per case. The JSON cases are the compatibility baseline in the same
run.

| Operation | Median ns/op | B/op | Allocs/op | Wire bytes | Raw ns/op samples |
| --- | ---: | ---: | ---: | ---: | --- |
| JSON encode | 930.0 | 496 | 2 | 426 | 963.8, 925.7, 932.8, 930.0, 916.3 |
| Binary encode | 216.2 | 112 | 1 | 108 | 213.0, 212.8, 217.5, 216.6, 216.2 |
| JSON decode | 6,051 | 792 | 15 | 426 | 6,038, 6,035, 6,051, 6,137, 6,227 |
| Binary decode | 291.1 | 272 | 5 | 108 | 284.4, 288.0, 291.1, 292.6, 293.5 |

Relative to JSON in this fixture, binary encoding is about 4.3x faster, binary
decoding about 20.8x faster, and the frame is 3.9x smaller. Per-operation
allocation is about 4.4x lower for encoding and 2.9x lower for decoding. This is a
codec benchmark, not an end-to-end `fsync` throughput benchmark; disk latency,
filesystem behavior, and network framing still need workload-specific testing.
