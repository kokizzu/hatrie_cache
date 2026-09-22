# C240 Read-Only Backup Attachment

C240 adds an immutable query view for backup data. It is useful for recovery
verification, incident investigation, ad-hoc reporting, and checking a backup
before promoting it, without publishing a writable restore directory.

## Supported Inputs

`OpenReadOnlyBackup` accepts:

- snapshot backup archives created with `BackupModeSnapshot`;
- Pebble checkpoint backup archives created with `BackupModePebbleCheckpoint`;
- incremental backup repositories created with `BackupModePebbleIncremental`;
- a raw snapshot file; and
- a native Pebble checkpoint directory, including a staged `cache.leveldb`
  directory.

Incremental repositories use the `latest` manifest by default. Set
`ReadOnlyBackupOptions.BackupID` to inspect a specific retained manifest.

## API

```go
attachment, err := hatCache.OpenReadOnlyBackup(
    "/srv/backups/cache-2026-09-23.tar.gz",
    hatCache.ReadOnlyBackupOptions{},
)
if err != nil {
    return err
}
defer attachment.Close()

value, ok, err := attachment.GetStringChecked("customer:42")
if err != nil {
    return err
}
if ok {
    fmt.Println(value)
}

result, err := attachment.QuerySQL(
    context.Background(),
    "FROM CACHE('people') AS person WHERE person.region = 'sg' SELECT person.id",
    nil,
    hatCache.SQLQueryOptions{},
)
```

Only read methods and `QuerySQL` are exposed. The caller cannot obtain the
backing mutable `HatTrie` or a writable persistent-store handle. `Close` is
idempotent and releases the loaded view plus any private temporary staging.

## Verification And Security

Archive attachments reuse the existing manifest, checksum, and safe-path
extractor. Repository attachments verify the repository descriptor and
content-addressed objects before opening the materialized Pebble view. The
caller-supplied data directory is never modified. Temporary extraction and
materialization are removed on `Close`, including error cleanup during open.

The attachment is a view of an immutable backup point. It loads the records
into a private in-memory trie, so changes made to a live cache after attach do
not change query results. It does not follow the live cache journal after
opening.

## Tradeoffs

- Open time and memory are proportional to the backup data loaded. This is not
  a lazy page-at-a-time reader.
- Archive and repository inputs need temporary disk space while their payloads
  are verified/materialized. The published restore data directory is avoided.
- The view is optimized for repeated reads after one open. For a single small
  lookup, opening the attachment can cost more than directly reading an
  already-open live cache.
- Encrypted persistent records still require the existing persistent-store
  cipher-aware recovery path; C240 does not expose keys or weaken encryption
  checks.
