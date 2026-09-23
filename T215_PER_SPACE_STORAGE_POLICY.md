# T215 Per-Space Storage Policy

T215 adopts Tarantool's useful distinction between a resident `memtx` space and
an LSM/Vinyl-style space. `hatDataStructure.Space` fixes the engine when the
space is created, so different spaces in one process can choose different
read/write tradeoffs without changing the default cache or table paths.

## Usage

The zero-value engine is bounded in-memory `memtx`:

```go
memtx, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
    Memtx: hatDataStructure.MemtxSpaceOptions{
        MaxRecords:    1_000_000,
        MaxValueBytes: 64 << 20,
    },
})
if err != nil {
    return err
}
if err := memtx.Put("customer:42", []byte("active")); err != nil {
    return err
}
```

An on-disk-oriented space uses the existing bounded LSM engine:

```go
vinyl, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
    Engine: hatDataStructure.SpaceEngineVinyl,
    Vinyl: hatDataStructure.LSMTableOptions{
        MemtableMaxRecords:      4096,
        MaxRunsBeforeCompaction: 8,
    },
})
if err != nil {
    return err
}
if err := vinyl.Put("customer:42", []byte("active")); err != nil {
    return err
}
if err := vinyl.Flush(); err != nil {
    return err
}
```

Both engines expose `Put`, `Get`, `Delete`, `Flush`, `Compact`, `Stats`, and
`MarshalBinary`. `UnmarshalSpace` restores the engine recorded in the
snapshot. A caller-supplied non-empty `SpaceOptions.Engine` must match it.
The snapshot is CRC-protected and memtx records are sorted before encoding, so
backup bytes are deterministic. The caller still owns writing snapshots to
durable storage, replication, encryption, and scheduling; there is no hidden
path or implicit fsync.

## Policy And Defaults

| Policy | Default | Strength | Cost or limitation |
| --- | --- | --- | --- |
| `SpaceEngineMemtx` | Yes | Lowest point-read latency; direct resident values | Uses process memory; default limit is 1,048,576 records and 64 MiB per value |
| `SpaceEngineVinyl` | No | Bounded memtable, sorted immutable runs, tombstones, compaction, compact snapshots | Point reads search runs; persistence is caller-owned through snapshots |

The engine is immutable after construction. To change policy, snapshot the
source space and restore it as a new space with the target engine. This avoids
silently changing memory and durability semantics for existing callers.

## Measurements

Linux/amd64, AMD Ryzen 9 5950X, `-benchmem`, three samples per case. The
workload performs a repeated put followed by get over 32 stable keys.

| Workload | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Direct unprotected map baseline | 76.1 | 16 | 2 | Raw latency control only |
| Direct locked/copying map control | 99.2 | 24 | 3 | Fair memtx concurrency/ownership control |
| `SpaceEngineMemtx` | 109.9 | 24 | 3 | 1.11x slower than fair control |
| Direct `LSMTable` baseline | 105.2 | 24 | 3 | Existing Vinyl control |
| `SpaceEngineVinyl` | 106.3 | 24 | 3 | 1.01x slower than direct control |

The first implementation measured `SpaceEngineVinyl` at 124.5 ns/op because
the wrapper took a second lock around an already thread-safe LSM table. That
redundant lock was removed before final measurement; the final Vinyl overhead
is within benchmark noise. Memtx retains one lock because its map backend is
owned by `Space` and must provide concurrent CRUD semantics. The new API is
opt-in, so existing unwrapped map and LSM users do not regress.

## Verification

```text
make test-t215
make test-t215-package
make benchmark-t215-before
make benchmark-t215
make race-t215
make vet-t215
```

The tests cover default and explicit engine selection, copied values, CRUD,
flush/compact behavior, deterministic engine-neutral snapshots, corruption and
engine-mismatch rejection, size limits, invalid options, and race detection.
