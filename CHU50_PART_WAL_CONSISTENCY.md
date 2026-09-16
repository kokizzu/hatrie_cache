# CH-U50: Part/WAL-Consistent Backup Manifest

Hatrie Cache backup manifests now have an optional ClickHouse-inspired
consistency section. It joins the immutable payload files, their SHA-256
checksums, the snapshot/checkpoint boundary, and the exact journal boundary in
one deterministic record.

This closes a recovery gap: a manifest can no longer describe a valid-looking
snapshot while silently referring to a different set of files or a different
write-ahead-log endpoint. The check is intentionally additive. Existing
manifests without `consistency` remain readable.

## Contract

`hat/hatBackup` exposes the contract to importers:

```go
consistency, err := hatBackup.BuildBundleConsistency(manifest)
if err != nil {
    return err
}
manifest.Consistency = consistency

if err := hatBackup.ValidateBundleConsistency(manifest); err != nil {
    return err
}
```

The public types are:

- `hatBackup.BundleConsistency`: version, boundaries, sorted parts, journal
  metadata, and the deterministic consistency digest.
- `hatBackup.BundlePart`: relative file path, kind, byte size, and SHA-256.
- `hatBackup.BundleJournalBoundary`: journal path, format, last sequence,
  byte size, and SHA-256.
- `hatBackup.BundleConsistencyVersion`: current schema version (`1`).

`BundlePart.Kind` is one of `snapshot`, `storage`, `metadata`, or `payload`.
The list includes every non-journal file in the manifest, sorted by relative
path. The journal is represented separately so its sequence boundary cannot be
confused with an immutable snapshot part.

## Boundary Semantics

- `PartSequence` is the sequence represented by the immutable snapshot or
  Pebble checkpoint.
- `JournalSequence` is the journal endpoint captured for the backup.
- For a normal full backup, the two values are normally equal.
- `BuildBundleConsistencyAtPartSequence` is used when a caller has a later
  journal endpoint but the immutable part was created at an earlier sequence.

Bundle creation records the consistency section after the payload files and
journal metadata are finalized and before a repository manifest is published
or its ID is calculated. Bundle and repository readers validate it before
accepting the manifest. Snapshot verification also compares the loaded
snapshot sequence with `PartSequence`; Pebble verification compares the
checkpoint's persisted applied-journal sequence with the same boundary.

The digest covers the consistency payload with its own `digest` field blanked
before hashing. This makes accidental edits, stale file lists, changed file
contents, and boundary drift fail deterministically. It is an integrity check,
not an authenticity mechanism: deployments that need tamper resistance still
need the existing encryption or an external signature/keyed verification
policy.

## Restore Behavior

Source bundle manifests are checked before extraction and again during the
normal snapshot/Pebble verification path. A mismatch fails before the restore
is treated as usable.

Point-in-time and selective-partition restores change the immutable file set or
the journal endpoint. Their derived staging manifest therefore clears the
source `consistency` section after the source has been validated. This avoids
claiming that a filtered result is still the original full backup. The source
manifest remains fully checked, and the derived state is verified using the
existing restore checks.

Older manifests with no `consistency` section retain the previous behavior and
are accepted for compatibility. New manifests should keep the section instead
of manually editing it; use `BuildBundleConsistency` after changing a complete
file list.

## Operator Checklist

1. Create the backup with the normal backup command or API.
2. Run the existing backup verification command before copying it off-host.
3. Keep the manifest and every referenced file together; do not rename files
   inside the archive or object-store layout.
4. Restore into an isolated directory and run the existing restore rehearsal.
5. Treat a consistency, checksum, sequence, or state-checksum error as a
   failed backup and retain the source for investigation.

For application-owned manifest processing, call
`hatBackup.ValidateBundleConsistency` before trusting a decoded
`BundleManifest`. The validator rejects unsafe paths, duplicate file joins,
invalid hashes, mismatched sizes, sequence drift, and a wrong consistency
digest.

## Measured Tradeoff

The benchmark uses five 100 ms samples per case on Linux/amd64 with an AMD
Ryzen 9 5950X and Go 1.26.6. Ratios below are `after / before`; lower time,
heap, allocations, and bytes are better. The fixture is intentionally small,
so manifest-only overhead is more visible than it is for a large backup.

| Operation | Before | After | Time ratio | Heap before/after | Allocs before/after | Extra wire/storage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Snapshot create | 3,669,805 ns | 4,255,124 ns | 1.16x | 1,412,421 / 1,416,830 B | 165 / 177 | bundle 600 / 718 B |
| Snapshot verify | 537,604 ns | 554,641 ns | 1.03x | 317,493 / 321,079 B | 280 / 307 | n/a |
| Pebble checkpoint create | 15,698,801 ns | 15,539,042 ns | 0.99x | 2,764,398 / 2,781,858 B | 1,583 / 1,618 | bundle 2,596 / 2,775 B |
| Manifest marshal | 853.1 ns | 1,208 ns | 1.42x | 1,105 / 1,618 B | 3 / 3 | manifest 600 / 1,036 B |

The practical cost is a small per-backup manifest and checksum pass. Snapshot
creation heap rises 0.31 percent and verification heap rises 1.13 percent in
this fixture. The Pebble checkpoint path is within normal benchmark noise for
CPU and adds 0.63 percent heap. Existing legacy manifests pay no new decode or
validation cost because the section is absent.

Reproduce the measurements with:

```sh
make prepare-chu50-benchmark-baseline-c203
make sync-chu50-benchmark-baseline-c203
make benchmark-chu50-before-c203
make benchmark-chu50-c203
```

The raw five-sample output is recorded in
`BENCHMARK.md#ch-u50-partwal-consistent-backup-manifest`.
