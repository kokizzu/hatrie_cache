# CH-U06 Persistent Lightweight Delete Bitmap

`hatDataStructure.PersistentDeleteBitmap` is a bounded, importable primitive
for immutable stored-part adapters that need logical deletes without rewriting
the entire row payload for every mutation.

```go
bitmap, err := hatDataStructure.NewPersistentDeleteBitmap(1_000_000)
if err != nil {
	return err
}
_, err = bitmap.Delete(42)
if err != nil {
	return err
}

snapshot, err := bitmap.MarshalBinary()
if err != nil {
	return err
}
restored, err := hatDataStructure.DecodePersistentDeleteBitmap(snapshot)
if err != nil {
	return err
}
visible := restored.Live()
deleted := restored.Contains(42)
```

## Contract

- The representation stores one bit per physical row in packed `uint64` words.
- `Delete` and `Undelete` are idempotent and return whether the bit changed.
- `MarshalBinary` and `DecodePersistentDeleteBitmap` use the versioned `HTDB1`
  format with little-endian words and a CRC32C checksum.
- `DecodePersistentDeleteBitmap` rejects bad magic/version, truncated or
  trailing bytes, inconsistent row/word/deleted counts, set bits outside the
  final row, checksum failures, and snapshots larger than
  `MaxPersistentDeleteBitmapBytes` (16 MiB).
- `UnmarshalBinary` validates completely before replacing the receiver, so a
  failed restore cannot partially change live state.
- The bitmap is not concurrency-safe. The owning table or part must hold its
  read/write lock around mutation and snapshot operations.
- The checksum detects corruption but is not authentication or encryption.
  A storage adapter must use its existing file permissions and encryption
  policy when persisting the snapshot.

This is the reusable persistent-part primitive. It does not silently enable
logical deletes, start a worker, or change the existing `TypedTable` default.
Automatic part-manifest/sidecar wiring remains the storage adapter's choice so
existing tables and backup formats are not changed implicitly.

## Measured Tradeoff

Five benchmark samples used 100,000 physical rows with every third row marked
deleted on an AMD Ryzen 9 5950X Linux/amd64 host. The baseline is an equivalent
byte-per-row state with the same CRC32C work.

| Operation | Byte-per-row baseline | Packed `HTDB1` | Improvement |
| --- | ---: | ---: | ---: |
| Encode CPU | 132,642 ns/op | 5,623 ns/op | 23.59x faster |
| Encode heap | 106,496 B/op | 13,568 B/op | 7.85x lower |
| Encode allocations | 1 | 1 | unchanged |
| Decode CPU | 59,893 ns/op | 4,809 ns/op | 12.45x faster |
| Decode heap | 106,497 B/op | 13,616 B/op | 7.82x lower |
| Decode allocations | 1 | 2 | 1 extra allocation |
| Snapshot size | 100,012 bytes | 12,522 bytes | 7.99x smaller |

The packed decoder allocates one words slice and one result object, while the
baseline allocates one byte-per-row boolean slice. The extra allocation is the
explicit tradeoff for substantially lower retained heap and storage/network
bytes. The implementation is not a compressed sparse bitmap: one bit per
physical row keeps random membership checks predictable and avoids worst-case
run expansion.

Raw five-sample output is recorded in [BENCHMARK.md](BENCHMARK.md#ch-u06-persistent-lightweight-delete-bitmap).

Run the focused checks with:

```text
make test-chu06-persistent-delete-bitmap
make race-chu06-persistent-delete-bitmap
make vet-chu06-persistent-delete-bitmap
make benchmark-chu06-persistent-delete-bitmap
```
