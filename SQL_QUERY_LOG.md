# SQL Query-Log Retention And Rotation

`hatSql.SQLQueryLog` stores privacy-safe terminal query records as newline-
delimited JSON. Query text, source names, parameters, result rows, and
cancellation reasons are not persisted. The log is opt-in through
`SQLQueryManagerOptions.QueryLog`.

## Defaults

`OpenSQLQueryLog(path)` and `SQLQueryLogOptions{}` preserve the existing
append-only behavior: one active file, no rotation, and no archive files.
Rotation is enabled only when `MaxFileBytes` or `MaxFileAge` is positive.

When rotation is enabled and `MaxRetainedFiles` is zero, seven numbered archive
files are retained by default. The active file plus those archives are bounded
by the configured limits. `MaxRetainedFiles` is capped at 1024.

## Configure

```go
log, err := hatSql.OpenSQLQueryLogWithOptions(
	"/var/lib/hatrie/query.log",
	hatSql.SQLQueryLogOptions{
		MaxFileBytes:     256 << 20,
		MaxFileAge:       24 * time.Hour,
		MaxRetainedFiles: 7,
	},
)
if err != nil {
	return err
}
defer log.Close()

manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
	HistoryCapacity: 1000,
	QueryLog:        log,
})
```

Rotation happens before the next append when either threshold is reached. Age
rotation therefore does not wake a process that is idle. A record larger than
`MaxFileBytes` is written to an empty segment so a single valid record cannot
be rejected solely because of the segment threshold.

Files use this layout:

```text
query.log       active segment
query.log.1     newest archived segment
query.log.2     next-oldest archived segment
...
```

The oldest archive is removed before newer archives are shifted. `Read()`
returns all available retained segments oldest-first, then the active segment.
Use the same retention options when reopening a rotated log so the expected
archive set is included. A malformed, symlinked, or non-regular archive is
rejected rather than followed or silently skipped.

## Operations

Call `Sync()` before a filesystem snapshot when `SyncOnAppend` is false. For a
consistent backup, capture the active file and every numbered archive together;
the query log is diagnostic data and is not part of the cache's core recovery
state. Restoring the files with their original names and opening the log with
the same options preserves chronological reads. Missing older archives simply
mean that the bounded retention window begins later.

The log directory is created with mode `0700` and log files with mode `0600`.
Rotation uses exclusive creation for a new active file, and reads reject
archive symlinks. Keep the log path on a trusted local filesystem and avoid
placing credentials or other sensitive values in surrounding operational
metadata.

The optional rotation path has extra filesystem work at segment boundaries.
The default path has no rotation checks beyond a disabled-feature branch. See
the [CH-004 benchmark](BENCHMARK.md#ch-004-retained-query-log-rotation) for
CPU, allocation, and rotation-cost measurements.
