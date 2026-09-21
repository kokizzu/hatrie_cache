# CH-024 Detach/Attach Parts

`hat/hatMerkle` now provides an opt-in `PartCatalog` for immutable-part
operations that need an operator-safe lifecycle:

1. `Attach` verifies a candidate through a caller-owned verifier before it is
   published as active.
2. `Detach` atomically removes an active name from the active set and retains
   its descriptor in quarantine.
3. A verified replacement can be attached under the same name without
   exposing the replacement before verification completes.
4. `Restore` can verify and republish the quarantined descriptor after a
   failed replacement or rollback.
5. `RemoveQuarantined` discards only lifecycle metadata; the caller owns file
   deletion and any backup retention policy.

The catalog stores only names, locations, manifests, and bounded lifecycle
metadata. It does not copy or retain part bytes. `AttachFile` uses
`VerifyImmutablePartFile`, which hashes the remaining bytes through an
`io.SectionReader` and preserves the file offset. The default catalog bound is
`1024` active plus quarantined descriptors; callers can set `MaxEntries`.

## Example

```go
catalog, err := hatMerkle.NewPartCatalog(hatMerkle.PartCatalogOptions{
    MaxEntries: 1024,
})
if err != nil {
    return err
}

entry := hatMerkle.PartCatalogEntry{
    Name:     "partition-2026-09-21",
    Location: "/srv/hatrie/parts/partition-2026-09-21.new",
    Manifest: manifest,
}
if err := catalog.AttachFile(entry, file); err != nil {
    return err
}

quarantined, err := catalog.Detach(entry.Name)
if err != nil {
    return err
}
// Move or inspect quarantined.Location, then call Restore with a verifier if
// rollback is required. The catalog never deletes the underlying file.
_ = quarantined
```

`Attach` and `Restore` require a non-nil verifier. For memory-backed bytes,
call `entry.Manifest.Validate(data)` from the verifier. For a file-backed part,
use `AttachFile` or call `VerifyImmutablePartFile` directly.

## Benchmark

Three samples were run on the repository's AMD Ryzen 9 5950X host with
`make benchmark-ch024-part-catalog`:

| Operation | Sample 1 | Sample 2 | Sample 3 | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| six-transition map lifecycle baseline | 156.5 ns | 149.3 ns | 155.1 ns | 0 | 0 |
| six-transition `PartCatalog` lifecycle | 359.1 ns | 360.0 ns | 356.3 ns | 0 | 0 |
| 1 MiB read-all checksum verification | 1.119 ms | 1.149 ms | 1.204 ms | 2,227,981-2,227,988 | 24 |
| 1 MiB streaming checksum verification | 572 us | 567 us | 564 us | 33,184 | 5 |

The lifecycle catalog costs about `2.4x` the plain-map loop because it adds
locking, bounded state, generation tracking, and clone-safe metadata. That is
an explicit operator-path tradeoff, not a replacement for a hot lookup map.
The streaming verifier is about `2x` faster and about `67x` lower in transient
bytes than reading the full part into memory. Existing storage behavior is
unchanged because the catalog is opt-in.
