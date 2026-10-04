# CH-U06 Persistent Delete Bitmap

CH-U06 adds `hatStorage.PersistentDeleteBitmap`, an opt-in stored-part delete
bitmap with a bounded `PDB1` binary frame. It validates the physical row
bound, supports mark/unmark/contains/ordered visiting, and protects persisted
bytes with CRC32C.

```go
bitmap := hatStorage.NewPersistentDeleteBitmap(1_000_000)
bitmap.Mark(42)
bitmap.Mark(43)

payload, err := bitmap.MarshalBinary()
if err != nil {
	panic(err)
}
restored, err := hatStorage.DecodePersistentDeleteBitmap(payload)
if err != nil {
	panic(err)
}
if !restored.Contains(42) {
	panic("delete was not restored")
}
```

The codec uses delta/run encoding for sparse or contiguous deletes. Once the
delete set crosses the run threshold, it promotes to dense 64-bit words and
copies that representation directly, so dense and random workloads do not pay
an additional allocation or a value-rebuild pass. The dense representation is
selected whenever it is no larger than the run frame. The default frame limit
is 64 MiB; oversized parts should be split by the storage layer.

The owning storage part is responsible for synchronization, manifest
publication, compaction, and choosing when to persist the frame. Existing SQL
typed-table snapshot defaults are unchanged; this is a reusable persistent
stored-part primitive.

## Measurement

The baseline is a dense 64-bit word frame with the same fixed header and CRC32C
checksum. Five `-count=5` samples were run on Linux/amd64, AMD Ryzen 9 5950X,
with one million physical rows.

| Workload | Baseline median | Candidate median | CPU | Wire size | Temporary bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 sparse deletes | 35,738 ns/op, 131,072 B/op, 1 alloc | 9,382 ns/op, 12,288 B/op, 4 allocs | 3.81x faster | 125,024 B -> 3,023 B, 41.4x smaller | 10.7x lower |
| 200,000 contiguous deletes | 34,159 ns/op, 131,072 B/op, 1 alloc | 34,296 ns/op, 131,072 B/op, 1 alloc | 1.00x | 125,024 B -> 125,024 B | neutral |
| 249,293 random deletes | 35,077 ns/op, 131,072 B/op, 1 alloc | 35,390 ns/op, 131,072 B/op, 1 alloc | 0.99x | 125,024 B -> 125,024 B | neutral |

Run the permanent benchmark with `make benchmark-chu06-persistent-delete-bitmap`.
Raw samples are recorded in `BENCHMARK.md`.
