# Region-Local Backup And Restore

Region-local backup is an opt-in snapshot workflow for deployments that route
keys by explicit prefixes such as `sg:` and `us:`. It creates a portable bundle
containing only keys covered by the requested prefixes. The normal `auto`
backup behavior remains a complete snapshot.

## Create

The monitoring server must be running and the request must identify both the
partition and its key prefixes:

```sh
make cli ARGS='backup -path backup/sg.tar.gz -mode snapshot -partition-local -partitions sg -partition-prefixes sg:'
```

The equivalent HTTP request body is:

```json
{
  "path": "backup/sg.tar.gz",
  "mode": "snapshot",
  "partition_local": true,
  "partition": {
    "partitions": ["sg"],
    "key_prefixes": ["sg:"],
    "topology_epoch": 42
  }
}
```

`partition_local` requires at least one `key_prefix`. The capture predicate is
applied before snapshot records are retained and encoded, so values remain in
their native snapshot representation and foreign keys are not transferred.
All supported snapshot value types and snapshot formats use the same path.

Local backup is deliberately limited to `snapshot` mode. `auto` selects the
same snapshot mode. Pebble checkpoints and incremental repositories are
whole-store artifacts; the command rejects `partition-local` with those modes
instead of producing an incomplete or unsafe Pebble database.

## Verify

Verification loads the snapshot and validates every recovered key against the
manifest prefixes:

```sh
make cli ARGS='doctor -path backup/sg.tar.gz'
```

The manifest contains `partition.local: true`. The doctor report must contain
`partition_validation.ok: true`; a manually altered bundle containing a key
outside the declared prefixes fails verification.

## Restore

Restore can require a matching partition selector. The selector is checked
before any destination staging or publication:

```sh
make restore-bundle \
  RESTORE_BUNDLE_PATH=backup/sg.tar.gz \
  DATA_DIR=data-sg \
  RESTORE_BUNDLE_PARTITIONS=sg
```

For the CLI directly:

```sh
make cli ARGS='restore-bundle -bundle backup/sg.tar.gz -data-dir data-sg -partitions sg -partition-prefixes sg:'
```

The selector must match the manifest partition IDs. Prefixes, topology epoch,
and topology fingerprint are checked when supplied. A selector cannot be used
against an older whole-store backup, and a selector for another region is
rejected. Omitting the selector preserves the historical restore behavior and
restores the bundle exactly as it was created.

Use the isolated rehearsal before exposing the restored data:

```sh
make restore-rehearsal \
  RESTORE_REHEARSAL_PATH=backup/sg.tar.gz \
  RESTORE_REHEARSAL_PARTITIONS=sg \
  RESTORE_REHEARSAL_PARTITION_PREFIXES=sg:
```

The restored directory is a region subset, not a complete replacement for a
multi-region installation. Restore each region into its own data directory and
start it with the matching partition routing and ownership configuration.

## Operational Rules

- Keep a complete snapshot or Pebble backup in addition to each local backup.
- Create local bundles from a node whose routing metadata and topology epoch are
  current.
- Treat the local bundle's journal as a checkpoint marker for the filtered
  snapshot; it is not a cross-region command-history archive.
- Verify and rehearse copied bundles before a disaster-recovery change.
- Keep backup files outside the live data directory and apply the normal backup
  permissions and authentication controls.
