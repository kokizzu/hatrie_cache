# CH-005 Delete Bitmap State Snapshots

Typed-table patch parts already use one packed delete bit per physical row.
This feature makes that logical-delete state restart-safe through a bounded,
versioned snapshot API:

```go
state, err := table.MarshalPatchState()
if err != nil {
	return err
}

// Restore the same physical rows first.
if err := table.RestorePatchState(state); err != nil {
	return err
}
```

`MarshalPatchState` is available only when `TypedTablePatchOptions.Enabled` is
true. `RestorePatchState` fails with
`ErrTypedTablePatchStateUnsupported` for a table without patch parts. Invalid
magic, version, checksum, dimensions, trailing bitmap bits, table names, row
counts, or physical key order return `ErrTypedTablePatchStateInvalid`.

The snapshot contains the table name, physical key order, deleted-row count,
and packed `uint64` bitmap words. It does not contain row values or schema
columns. Restore validates the complete payload before replacing the current
bitmap, so a failed or corrupt restore cannot partially change the table.
Snapshots are capped at `MaxTypedTablePatchStateBytes` (16 MiB) before decode
allocations are admitted.

## Tradeoff

The key list makes a bitmap safe to restore against a different process: a
bitmap position is never applied to a different physical key order. Restore
compares encoded key bytes directly while holding the table lock, avoiding one
allocation per key. The immutable stored-part manifest binding is now
available through `hatMerkle.PartDeleteBitmap` and
`PartManifest.DeleteBitmap`; automatic part discovery and cross-process storage
orchestration remain caller-owned.

## Measurement

The fixture has 4,096 physical rows and 1,024 logical deletes. Five samples
run on an AMD Ryzen 9 5950X Linux/amd64 host.

| Path | Before optimization | Final | Improvement |
| --- | ---: | ---: | ---: |
| Marshal CPU | 28,728 ns/op | 28,289 ns/op | 1.02x faster, within noise |
| Marshal heap | 32,768 B/op | 32,768 B/op | unchanged |
| Marshal allocations | 1 | 1 | unchanged |
| Restore CPU | 295,597 ns/op | 25,452 ns/op | 11.61x faster |
| Restore heap | 299,939 B/op | 520 B/op | 576.81x lower |
| Restore allocations | 4,106 | 2 | 2,053x fewer |

The before values are the first implementation's key-string parsing path. The
final path compares encoded key bytes directly. Raw samples are in
[BENCHMARK.md](BENCHMARK.md#ch-005-delete-bitmap-state-snapshots).

## Immutable-part manifest binding

`TypedTable.MarshalPatchStateWithManifest` returns the exact serialized bitmap
and a `hatMerkle.PartDeleteBitmap` containing the physical row count, deleted
row count, and SHA-256 snapshot checksum. Attach that descriptor to an
immutable `hatMerkle.PartManifest` without copying bitmap bytes into the part
catalog:

```go
snapshot, bitmap, err := table.MarshalPatchStateWithManifest()
if err != nil {
	return err
}
manifest := hatMerkle.PartManifest{
	Checksum:     hatMerkle.ChecksumPart(partBytes),
	DeleteBitmap: &bitmap,
}
if err := bitmap.Verify(snapshot); err != nil {
	return err
}
```

`PartCatalog` persists this optional metadata in checkpoint format version 2
and continues to restore version-1 checkpoints. The bitmap payload remains a
separate bounded snapshot, so catalog restore does not allocate or retain row
data. A nil descriptor preserves the legacy manifest and checkpoint path.
