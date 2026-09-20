# T-U17 Selectable Vinyl-Style LSM Table

`hatDataStructure.LSMTable` is an opt-in byte-key/byte-value table inspired by
Tarantool Vinyl and ClickHouse immutable-part storage. It combines a mutable
memtable with immutable `SealedUpsertRun` values:

- `Put` and `Delete` copy caller-owned values into the memtable.
- A bounded memtable flush creates a CRC-protected, front-coded sorted run.
- Reads search the memtable and then newest runs first; tombstones mask older
  values.
- Automatic compaction is bounded by `MaxRunsBeforeCompaction` and removes
  obsolete values and tombstones after folding older history.
- `MarshalBinary` and `UnmarshalLSMTable` snapshot and validate the complete
  run set without exposing internal buffers.

## Usage

```go
table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
    MemtableMaxRecords:      4096,
    MaxRunsBeforeCompaction: 8,
    RunOptions: hatDataStructure.SealedUpsertRunOptions{
        MaxRecords:    1_000_000,
        MaxValueBytes: 64 << 20,
    },
})
if err != nil {
    return err
}
if err := table.Put("customer:42", []byte("active")); err != nil {
    return err
}
value, ok := table.Get("customer:42")
```

The default engine is unchanged. Choose this table only when batched writes,
bounded sorted runs, compact binary snapshots, or explicit compaction are more
important than the lowest possible point-read latency. It has no implicit disk
path, replication policy, or backup scheduling; callers own those operations.

## Measured Tradeoff

Measurements were taken on Linux/amd64, AMD Ryzen 9 5950X, three
`-benchmem` samples. The map comparison uses the same hot-key upsert workload
for writes and the same 100,000-row point lookup workload for reads.

| Workload | Map control | LSM table | Relative result | Memory |
| --- | ---: | ---: | ---: | --- |
| Hot-key upsert | 44.46 ns/op | 57.60 ns/op | 1.30x slower | 48 -> 48 B/op; 1 -> 1 alloc/op |
| Point lookup after compaction | 10.05 ns/op | 369.9 ns/op | 36.8x slower | 0 -> 28-29 B/op; 0 -> 2 allocs/op |
| Eight-run compaction, 16,000 records | N/A | 13.431 ms | LSM maintenance cost | 9.13 MB/op; 32,218 allocs/op |
| Snapshot of 10,000 records | N/A | 104,376 wire bytes | 10.4 wire bytes/record | 213 KB/op; 2-3 allocs/op |

`ImmutableBytes` and the snapshot byte count describe encoded run storage, not
the full Go heap retained by sparse restart keys and object headers. The large
point-read penalty is intentional and documented; the table remains opt-in and
must not replace the default map for latency-sensitive reads.

## Verification

```text
make test-tu17
make verify-tu17
make benchmark-tu17
```

The tests cover flush ordering, overwrites, tombstones, automatic compaction,
snapshot restore, corruption rejection, invalid writes, and nil receivers.
