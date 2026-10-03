# CH-U06 Persistent Delete Bitmap

`TypedTable.MarshalDeleteBitmap` and `TypedTable.RestoreDeleteBitmap` provide
an opt-in compact persistence format for logical deletes in typed-table parts.
The existing `MarshalPatchState` and `RestorePatchState` APIs remain unchanged
for callers that need the full physical-key payload.

## Format

The binary payload contains a versioned `HTDB1` header, table name, physical
row count, deleted-row count, a SHA-256 fingerprint of the ordered physical
keys, packed little-endian `uint64` delete words, and a CRC32 checksum. Restore
validates the complete payload and the current table identity before changing
the bitmap. Corrupt, truncated, oversized, wrong-schema, and wrong-row-order
payloads are rejected without changing existing delete state.

The fingerprint is cached while physical row order is unchanged. Appending a
row, physically deleting a row, or compacting patch parts invalidates it;
ordinary logical deletes do not. Patch parts must be enabled. The encoded state
is bounded by `MaxTypedTableDeleteBitmapBytes` (16 MiB by default).

## Example

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name:       "events",
    PatchParts: hatSql.TypedTablePatchOptions{Enabled: true},
    Columns:    []hatSql.TypedTableColumn{{Name: "value", Kind: hatSql.TypedTableInt64}},
})
if err != nil {
    return err
}

// Rebuild the same physical rows in the same order before restoring the mask.
state, err := table.MarshalDeleteBitmap()
if err != nil {
    return err
}
if err := table.RestoreDeleteBitmap(state); err != nil {
    return err
}
```

In a backup workflow, store the bitmap beside the stored part and restore the
part's rows first. Use the existing full patch snapshot when the physical key
order cannot be reproduced or when its per-key mismatch diagnostics are more
important than the compact payload.

## Tradeoff

Five samples on Linux/amd64 with an AMD Ryzen 9 5950X used 4,096 rows and 64
packed bitmap words. The full snapshot carried the physical keys;
the compact snapshot carried only the fingerprint and bitmap words.

| Operation | Existing full state | Compact state | Result |
| --- | ---: | ---: | --- |
| Snapshot bytes | 32,205 | 579 | 55.6x smaller |
| Warm marshal | 22.5-24.9 us, 32,768 B/op, 1 alloc | 0.34-0.38 us, 640 B/op, 1 alloc | about 67x faster |
| Warm restore | 24.5-26.4 us, 520 B/op, 2 allocs | 0.31-0.33 us, 520 B/op, 2 allocs | about 80x faster |
| Cold compact marshal after row-order change | 22.5-24.9 us | 67.9-71.3 us, 640 B/op, 1 alloc | about 2.8x slower once per fingerprint rebuild |

The compact format is retained because repeated checkpoint/backup writes are
the target path: it removes almost all stored bytes and the cached path is
faster with no additional restore allocation. The cold fingerprint cost is
explicit and bounded; callers with infrequent one-shot snapshots can continue
using the existing full format.

Focused correctness, race, vet, and benchmark coverage is in
`hat/hatSql/ch_u06_persistent_delete_bitmap_*_test.go`.
