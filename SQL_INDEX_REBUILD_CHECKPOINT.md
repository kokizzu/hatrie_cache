# SQL Index Rebuild Checkpoints

SQL JSON index rebuild requests are in-memory by default. An interrupted
process therefore loses its queued rebuilds. The optional checkpoint store
keeps each `(cache key, field)` rebuild durable until the atomic rebuild unit
completes, which is useful for large indexes and rolling restarts.

## Enable

Configure the index, attach the store, then schedule rebuilds or start the
existing background worker:

```go
store, err := hatriecache.NewFileSQLJSONIndexRebuildCheckpointStore(
    "data/sql-index-rebuild-checkpoints.json",
)
if err != nil {
    return err
}
if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
    return err
}
if err := trie.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), store); err != nil {
    return err
}
if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
    return err
}
_, err = trie.RunScheduledSQLJSONIndexRebuilds(1)
return err
```

On the next process, create the same source and index definitions and attach a
new store for the same path. Pending checkpoints are queued automatically. If
the store is attached before index definitions are restored, call
`ResumeSQLJSONIndexRebuilds()` after the definitions are registered.

The checkpoint file contains only cache keys and field names. It does not
contain source values or indexed rows. The file store rejects malformed,
unsupported-version, symlink, and non-regular files on load. Saves use a
private `0600` temporary file, flush the file and directory, then atomically
rename it into place. A missing file means no pending rebuilds.

## Failure Semantics

- The default is disabled: no checkpoint file is created and ordinary SQL
  reads, writes, and index rebuilds keep the prior behavior.
- Scheduling persists the request before returning when a store is enabled.
- A request remains durable while it is running, so a crash retries it after
  restart.
- A successful rebuild removes its checkpoint. If that removal cannot be
  persisted, the request remains queued and can be retried safely.
- A custom `SQLJSONIndexRebuildCheckpointStore` must make `Save` durable before
  returning and must not call back into the trie from `Load` or `Save`.
- One process should own a file checkpoint path. Use an external store
  implementation when multiple processes need shared ownership.

This is a whole-index checkpoint, not a row cursor. The existing index refresh
publishes an index atomically; a retry may rebuild the unit again, but readers
do not observe a partially published index.

## Cost

The measured benchmark runs one schedule and one rebuild per operation over a
single-row JSON source. On Linux amd64 with an AMD Ryzen 9 5950X, the disabled
path is the baseline. The file-store result includes two atomic durable saves
per operation and is intentionally much slower; this is an operational
recovery feature, not a hot-path optimization. The final measured durable
file path is about `2.82 ms` per schedule/run on the benchmark host. See the
raw samples in
[BENCHMARK.md](BENCHMARK.md#tt-041-sql-index-rebuild-checkpoints).
