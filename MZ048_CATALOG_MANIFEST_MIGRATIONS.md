# MZ048 Persisted Catalog Manifest Migrations

MZ048 gives `hatSchema.SpaceCatalog` a durable metadata boundary. It stores a
versioned, normalized manifest with a SHA-256 checksum and upgrades older
manifests through bounded one-version migration steps. Publication writes a
0600 temporary file, flushes it, renames it over the destination, and flushes
the parent directory.

This is deliberately separate from `hatBackup.BackupManifestCatalog`: that
catalog tracks backup-chain objects, while MZ048 tracks named spaces, schema
versions, constraints, and index declarations.

## Basic Usage

```go
store, err := hatSchema.NewSpaceCatalogManifestStore(
	"/var/lib/hatrie/catalog-manifest.json",
	hatSchema.SpaceCatalogManifestStoreOptions{},
)
if err != nil {
	return err
}

manifest, err := store.LoadAndMigrate()
if err != nil {
	return err
}

manifest.Generation++
manifest.Spaces = append(manifest.Spaces, hatSchema.SpaceDefinition{
	Name: "orders",
})
manifest.Version = hatSchema.SpaceCatalogManifestVersion
if err := store.Publish(manifest); err != nil {
	return err
}
```

`Publish` normalizes and sorts spaces by name, validates every definition using
the existing `SpaceCatalog` rules, and refuses a version other than the
store's target version. `Load` validates the checksum envelope but does not
run migrations. `LoadAndMigrate` performs all registered steps in memory and
publishes only after the final version and schema validate.

## Migration Steps

The default migrator includes the legacy direct-JSON v0 to v1 step. Future
steps must advance exactly one version:

```go
migrator := hatSchema.NewSpaceCatalogManifestMigrator()
err := migrator.Register(1, func(m hatSchema.SpaceCatalogManifest) (hatSchema.SpaceCatalogManifest, error) {
	// Transform fields introduced by manifest version 2.
	m.Version = 2
	return m, nil
})
if err != nil {
	return err
}

store, err := hatSchema.NewSpaceCatalogManifestStore(path,
	hatSchema.SpaceCatalogManifestStoreOptions{
		Migrator:      migrator,
		TargetVersion: 2,
	})
```

Migration callbacks receive detached values. A missing step, duplicate
registration, version regression, invalid definition, or callback error leaves
the original file untouched. Migrations are bounded to 64 manifest versions;
the default file limit is 8 MiB and can be raised only to the hard 64 MiB
bound.

## Safety Boundaries

- The store rejects symlink and non-regular destination paths.
- Existing files are read only after the same path validation.
- Checksums are compared in constant time after canonical JSON encoding.
- Temporary files are created with exclusive creation and mode 0600.
- No migration callback runs while reading arbitrary files or holding a live
  `SpaceCatalog` lock.
- Store operations serialize per store instance; multi-process callers still
  need external ownership or locking for one path.
- A checksum proves file integrity, not source authenticity; deployments that
  need authenticity must protect the directory or add an authenticated outer
  transport.

## Cost And Measurement

The benchmark uses 64 named spaces on an AMD Ryzen 9 5950X. Raw JSON is the
pre-MZ048 baseline and does not validate definitions or include a checksum.

| Operation | Raw JSON median | MZ048 median | Relative CPU | Raw memory | MZ048 memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Encode | 13,992 ns/op | 18,401 ns/op | 1.32x slower | 4,923 B/op | 10,324 B/op |
| Decode | 86,731 ns/op | 188,596 ns/op | 2.17x slower | 19,176 B/op | 35,478 B/op |

Allocations were 2 versus 3 for encode and 209 versus 241 for decode. This
cost is paid only during manifest persistence or recovery; no existing
`SpaceCatalog` lookup/upsert path serializes or hashes data.

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
raw encode: 13566 4923 2
raw encode: 14629 4922 2
raw encode: 14572 4922 2
raw encode: 13992 4923 2
raw encode: 13300 4923 2
MZ048 encode: 18401 10322 3
MZ048 encode: 18341 10327 3
MZ048 encode: 17464 10322 3
MZ048 encode: 18560 10323 3
MZ048 encode: 18523 10324 3
raw decode: 83666 19176 209
raw decode: 85537 19176 209
raw decode: 93024 19176 209
raw decode: 86731 19176 209
raw decode: 87384 19176 209
MZ048 decode: 180712 35486 241
MZ048 decode: 193838 35478 241
MZ048 decode: 189146 35478 241
MZ048 decode: 180895 35455 241
MZ048 decode: 188596 35485 241
```

Run it with:

```text
make benchmark-mz048-space-catalog-manifest
```

Focused correctness and package checks are available through:

```text
make test-mz048-space-catalog-manifest
make test-mz048-package
make race-mz048-space-catalog-manifest
make vet-mz048-space-catalog-manifest
```
