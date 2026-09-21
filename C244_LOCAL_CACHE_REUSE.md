# C244: Local Cache Reuse Validated by Part and Column Checksums

`hatMerkle.PartManifest` provides the metadata boundary needed by a local
immutable-part cache. It records one whole-part `PartChecksum` plus an
independent checksum for each named, non-overlapping column range.

```go
ranges := []hatMerkle.PartColumnRange{
	{Name: "key", Offset: 0, Size: keyBytes},
	{Name: "value", Offset: keyBytes, Size: valueBytes},
}
manifest, err := hatMerkle.BuildPartManifest(part, ranges)
if err != nil {
	return err
}

// Validate bytes when admitting a local entry or after an untrusted copy.
if err := manifest.Validate(part); err != nil {
	return err
}

// Reuse is a metadata-only, allocation-free comparison on the hot path.
if cachedManifest.Equal(manifest) {
	return reuseCachedPart()
}
```

The whole-part checksum catches modifications outside the recorded columns;
column checksums make a changed column distinguishable. Invalid ranges,
overlap, duplicate names, truncation, and changed bytes are rejected. Existing
backup, replication, and cache paths remain unchanged because this is an
explicit opt-in primitive.

## Measured Cost

On the benchmark host, a 1 MiB whole-part SHA-256 costs `473,871-550,542
ns/op`, with `0 B/op` and `0 allocs/op`. A four-column manifest validation
costs `960,479-1,051,332 ns/op`, also with zero allocations, or about `1.87x`
the median whole-part hash. That validation cost is paid only at cache
admission or integrity verification. Reuse compares manifests in
`33.79-36.94 ns/op`, with zero allocations, because it does not reread or
rehash the cached bytes.

The feature is therefore not enabled as a hidden extra hash on existing hot
paths. Callers choose when local bytes need validation and can use the cheap
manifest comparison for repeated reuse.
