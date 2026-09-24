# M-U38: Persisted Immutable Part Catalog

Hatrie Cache now has an opt-in durable catalog for immutable part metadata.
`hatMerkle.PartCatalog` still owns no part bytes: it records the part name,
location, lifecycle generation, whole-part checksum, and optional per-column
checksums. This makes active and quarantined parts recoverable without copying
large data files into the checkpoint.

## API

```go
catalog, err := hatMerkle.NewPartCatalog(hatMerkle.PartCatalogOptions{
	MaxEntries: 4096,
})
if err != nil {
	return err
}

manifest, err := hatMerkle.BuildPartManifest(partBytes, []hatMerkle.PartColumnRange{
	{Name: "key", Offset: 0, Size: 8},
	{Name: "value", Offset: 8, Size: uint64(len(partBytes) - 8)},
})
if err != nil {
	return err
}
if err := catalog.Attach(hatMerkle.PartCatalogEntry{
	Name:     "orders-0001",
	Location: "parts/orders-0001.bin",
	Manifest: manifest,
}, func(entry hatMerkle.PartCatalogEntry) error {
	return entry.Manifest.Validate(partBytes)
}); err != nil {
	return err
}

// Save is atomic and durable for the checkpoint file.
if err := catalog.Save("state/parts.catalog"); err != nil {
	return err
}

recovered, err := hatMerkle.LoadPartCatalog(
	"state/parts.catalog",
	hatMerkle.PartCatalogOptions{MaxEntries: 4096},
)
```

`MarshalBinary` and `RestorePartCatalog` are the in-memory equivalents when a
caller already owns the checkpoint bytes. `Snapshot`, `Get`, and
`GetQuarantined` expose copies of the recovered metadata. A quarantine entry is
still only a lifecycle record; deleting the underlying part remains an
explicit caller operation. The existing remote-part garbage-collection
planner can consume caller-maintained reachability derived from these records.

## Format and recovery guarantees

- The versioned `HPCK` format is deterministic: active and quarantined entries
  are sorted by name before encoding.
- The payload contains whole and column SHA-256 checksums and is protected by a
  CRC32C checksum. Truncation, trailing bytes, unsupported versions, invalid
  ranges, duplicate column names, and checksum-size mismatches are rejected.
- `Save` writes a temporary file in the destination directory, sets mode
  `0600`, flushes file data, atomically renames it, and flushes the directory.
  A failed save removes its temporary file and does not replace the previous
  checkpoint.
- Restore is bounded to a 64 MiB checkpoint, 100,000 entries, 65,536 columns
  per entry, and 1 MiB per encoded string. `LoadPartCatalog` limits the read
  before decoding, so a malformed oversized file cannot request unbounded
  memory.
- Restoring metadata does not verify the part bytes. Call `Attach`, `Restore`,
  `AttachFile`, or `PartManifest.Validate` at the point where a part is opened
  or reused to verify the referenced immutable storage.

## Measured cost

Five `-benchtime=100ms` samples on Linux/amd64 with an AMD Ryzen 9 5950X. The
fixture has 256 catalog entries, 64 of them quarantined, and a 53,572-byte
checkpoint.

| Operation | Median time | B/op | Allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| Existing `Snapshot` baseline | 75.2 us | 64,496 | 264 | In-memory copy only |
| `MarshalBinary` | 181.4 us | 296,560 | 269 | CRC and full metadata encoding |
| `RestorePartCatalog` | 93.1 us | 94,544 | 1,291 | No filesystem I/O |
| `LoadPartCatalog` | 150.1 us | 224,657 | 1,311 | Read plus restore |
| `Save` | 3.04 ms | 297,512 | 283 | Atomic rename plus file and directory sync |

The checkpoint path therefore adds work only when a caller checkpoints or
recovers the catalog. Ordinary attach, detach, lookup, and part-byte storage
remain unchanged. `Save` is intentionally slower than `MarshalBinary` because
it pays the durability cost needed for crash recovery; callers that only need
transport bytes can use `MarshalBinary` instead.

Raw benchmark output is recorded in [BENCHMARK.md](BENCHMARK.md#mu38-persisted-immutable-part-catalog).
