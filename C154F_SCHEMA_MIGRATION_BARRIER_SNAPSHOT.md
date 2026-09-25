# C154f schema migration barrier snapshots

C154f adds an opt-in binary snapshot format for `hatPipeline.SchemaMigrationBarrier`.
It is intended for durable checkpoints, restart recovery, and transfer between
compatible nodes. The normal prepare/acknowledge/commit/abort path is unchanged.

## API

```go
payload, err := barrier.MarshalSnapshot()
if err != nil {
	return err
}

if err := barrier.RestoreSnapshot(payload); err != nil {
	return err
}
```

`MarshalSnapshot` is deterministic: barrier IDs are sorted and each record
retains the normalized dependency order. `RestoreSnapshot` fully decodes and
validates the input before taking the write lock, then atomically replaces the
registry. A failed restore leaves the existing registry unchanged.

## Format and safety

- Magic/version: `SMB1`, version `1`.
- Big-endian fixed-width counts and versions.
- Each barrier stores its ID, version, state, dependencies, and acknowledged
  dependency indexes.
- A CRC32 checksum covers every byte before the checksum.
- Payloads are capped at 64 MiB.
- Existing barrier limits still apply: at most 100,000 barriers, 1,024
  dependencies per barrier, and 256 bytes per text field.
- IDs and dependencies must be non-empty, trimmed, unique, and sorted in the
  canonical representation.
- Prepared, committed, and aborted states are checked; a committed barrier
  must have acknowledged every dependency.
- Duplicate IDs, malformed lengths, invalid indexes, trailing bytes, bad
  checksums, and unsupported versions are rejected.

The format is an internal compatibility contract for this package version. A
future incompatible format must use a new magic/version and keep the current
decoder strict.

## Benchmark

Command:

```text
make benchmark-c154f-schema-barrier-snapshot
```

Fixture: 32 barriers, four dependencies per barrier, and a mix of prepared,
committed, and aborted states. Each number below is the median of five
200-ms samples on the same AMD Ryzen 9 5950X host.

| Operation | JSON baseline | Binary snapshot | Improvement |
| --- | ---: | ---: | ---: |
| Snapshot encode | 16,144 ns/op, 13,119 B/op, 58 allocs/op, 5,659 wire bytes | 7,852 ns/op, 2,560 B/op, 2 allocs/op, 1,852 wire bytes | 2.06x faster, 5.12x lower allocation bytes, 29x fewer allocs, 3.06x smaller wire payload |
| Full registry restore | 90,742 ns/op, 34,960 B/op, 554 allocs/op, 5,659 wire bytes | 18,070 ns/op, 24,232 B/op, 368 allocs/op, 1,852 wire bytes | 5.02x faster, 1.44x lower allocation bytes, 1.51x fewer allocs, 3.06x smaller wire payload |

The JSON restore baseline unmarshals the same status data and rebuilds the same
bounded registry maps, so it is comparable to `RestoreSnapshot`; it is not just
a detached decode benchmark. Binary restore additionally verifies CRC and
strict format/state invariants.

Raw five-sample results:

```text
BenchmarkC154fSchemaBarrierJSONSnapshotBaseline: 16042, 16187, 16036, 16363, 16144 ns/op; 13126, 13119, 13116, 13120, 13116 B/op; 58 allocs/op; 5659 wire-bytes
BenchmarkC154fSchemaBarrierBinarySnapshot:       7616,  7633,  7852,  7973,  8064 ns/op;  2560 B/op; 2 allocs/op; 1852 wire-bytes
BenchmarkC154fSchemaBarrierJSONRestoreBaseline:  92299, 90577, 89853, 90742, 91006 ns/op; 34960 B/op; 554 allocs/op; 5659 wire-bytes
BenchmarkC154fSchemaBarrierBinaryRestore:        18070, 17803, 18481, 17886, 18246 ns/op; 24232 B/op; 368 allocs/op; 1852 wire-bytes
```

The result is a serialization and transfer win. It does not change the normal
in-memory barrier hot path, and restore still allocates the maps required for
live barrier operations.
