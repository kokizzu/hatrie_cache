# CH-011 Durable Projection Metadata

This is an opt-in persistence slice inspired by ClickHouse projection DDL.
`SQLSession` still keeps materialized rows in memory, but projection
definitions can now survive a process restart and be rebuilt against the
current source version.

## Usage

```go
store, err := hatSql.NewFileSQLProjectionDefinitionStore("/var/lib/hatrie/projections.spc")
if err != nil {
	return err
}
session, err := hatSql.NewSQLSessionWithOptions(source, hatSql.SQLSessionOptions{
	ProjectionDefinitionStore: store,
	AutoRestoreProjections:    true,
})
if err != nil {
	return err
}
```

Existing sessions can call `SetProjectionDefinitionStore` before projection
DDL. `RestoreProjections(ctx)` is available when restore should be explicit
instead of automatic. `CREATE PROJECTION` and `DROP PROJECTION` update the
definition snapshot; `REFRESH PROJECTION` changes rows but not the definition.

## Format And Safety

The file store uses a deterministic `HSP1` binary snapshot with length-prefixed
strings and a CRC-32 checksum. Writes use a `0600` temporary file, `fsync`, and
an atomic rename. Parent directories are created with mode `0750`. Defaults
bound a snapshot to 4,096 definitions and 8 MiB; custom limits are available
through `NewFileSQLProjectionDefinitionStoreWithOptions`.

Only definitions are persisted. Restore always executes the query against the
configured resolver and checks its source-version contract, so a stale cached
row cannot be served after restart. Missing files mean an empty catalog;
malformed or checksum-invalid files fail closed with
`ErrSQLProjectionDefinitionStoreCorrupt`.

## Measured Tradeoff

The default constructor remains in-memory and pays no persistence cost. On the
benchmark machine, the exact projection-hit median stayed effectively flat at
about 15.39 us before and 15.09 us after; the full-scan median was 7.03 ms
before and 6.95 ms after. The new opt-in DDL cost was about 0.92 ms, 2.25 KiB,
and 25 allocations per durable save, plus 10.27 us, 1.65 KiB, and 34
allocations per load. Save cost is intentionally paid on DDL, not on reads.

Run the reproducible benchmark with:

```text
make benchmark-ch011-projection-persistence
```

Automatic source-change notifications, cross-process coordination, and
persistent materialized row parts remain future work.
