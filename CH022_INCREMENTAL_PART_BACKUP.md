# CH-022 Incremental Part Backup

`hatBackup.ObjectStoreTarget` now supports content-addressed payload objects
for incremental backups. This is inspired by ClickHouse immutable parts: the
manifest keeps the original file paths, while unencrypted payloads are stored
once under `objects/<sha256>` and can be referenced by multiple backup
manifests. Encrypted payloads use `objects/<key-id>/<sha256>` so a key rotation
never reuses ciphertext under a manifest that declares a different key.

## Defaults

`ObjectStoreLayoutAuto` is the default. It keeps the historical path layout
for snapshots and selects `ObjectStoreLayoutContentAddressed` when
`BundleManifest.Mode` is `ModePebbleIncremental`.

The old layout remains an explicit fallback:

```go
target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup",
    hatBackup.ObjectStoreTargetOptions{
        Layout: hatBackup.ObjectStoreLayoutPath,
    })
```

To force the new layout for any backup:

```go
target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup",
    hatBackup.ObjectStoreTargetOptions{
        Layout: hatBackup.ObjectStoreLayoutContentAddressed,
    })
```

## Reuse

Stores may implement the optional `hatBackup.ObjectStoreObjectExists`
interface. When available, a content-addressed backup hashes each source file
and checks the content-addressed object path before uploading. Existing objects
are counted as reused and are not transferred. Duplicate content within one backup is
also uploaded only once even when the store has no existence API.

The returned `BundleManifest` records `ObjectLayout`, `NewObjectHashes`,
`ReusedObjectHashes`, object counts, and plaintext byte totals. `Files` still
contains every logical restored path, so restore and verify reconstruct and
check the complete tree rather than only the changed files.

## Restore And Security

Restore and verify select the object key from the manifest layout. A content
hash is validated as a SHA-256 value before it is used as an object key, and
the downloaded plaintext is checked for both the declared size and digest.
Restore writes into an isolated staging directory and publishes only after
all files pass verification. Encrypted content-addressed objects use the
key-qualified content object key as authenticated-data context, allowing the
same encrypted object to serve more than one logical path without allowing
ciphertext from a rotated key to be mistaken for the active key.

The source should remain quiescent for the duration of a backup, as with the
historical path layout. If a source file changes between the hash pass and the
upload pass, the backup fails instead of publishing a manifest for the
changed content.

## Tradeoffs

Content addressing performs a hash pass before a new object upload, so a cold
backup reads a changed file twice. It does not buffer the file in memory. On
an unchanged incremental backup, the extra work is one hash pass per source
file plus the store existence checks; payload transfer drops to zero for
reused objects. The path layout avoids the extra hash pass and is useful for
stores without an efficient existence check or for compatibility-sensitive
consumers.

`PlanBackupChain` and `PlanBackupRetention` continue to validate complete
manifest chains and protect objects referenced by retained manifests. The
object-store target still publishes the latest manifest at `manifest.json`;
callers that retain multiple backup IDs should retain the returned manifests
or publish them in their catalog for chain planning. `KeepObjectKeys` and
`DeleteObjectKeys` are the exact relative object addresses to use for physical
cleanup; this matters when the same plaintext hash exists under multiple
encryption key IDs. The older `KeepObjectHashes` and `DeleteObjectHashes`
fields remain content-digest summaries.

Benchmark details and raw samples are recorded in
[BENCHMARK.md#ch-022-incremental-part-backup](BENCHMARK.md#ch-022-incremental-part-backup).
