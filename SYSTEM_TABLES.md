# SQL System Tables

`NewSQLSystemTablesResolver` exposes a small, read-only operational catalog to
the SQL executor. It wraps an existing `hatCache.SQLSourceResolver`, so normal
`CACHE`, `KEYS`, and other sources keep their existing behavior.

```go
resolver := hatCache.NewSQLSystemTablesResolver(trie,
    hatCache.SQLSystemTablesResolverOptions{
        QueryManager: queryManager,
        Journal:      commandJournal,
    })
result, err := hatCache.ExecuteSQLQuery(
    "FROM CACHE('system.parts') SELECT name, rows",
    resolver,
)
```

The wrapper is intentionally explicit. It does not start a server, change
authentication, or expose a system table unless the application installs the
wrapper as its SQL source resolver.

## Tables

### `system.parts`

This table describes the in-memory cache layout:

| Column | Type | Meaning |
| --- | --- | --- |
| `name` | string | `root` for an unpartitioned trie, or a local partition name such as `local-000`. |
| `partition` | int64 | Zero-based local partition number. The unpartitioned root is `0`. |
| `rows` | int64 | Current number of keys in the root or local partition. |
| `active` | bool | Always `true` for currently exposed parts. |

An unpartitioned cache returns one `root` row. A cache configured with local
partitions returns one row per local partition. This is layout metadata, not a
snapshot and not a consistency barrier for concurrent writes.

Example:

```sql
FROM CACHE('system.parts')
SELECT name, partition, rows, active
ORDER BY partition
```

### `system.mutations`

This table exposes the bounded tail of a configured `CommandJournal`:

| Column | Type | Meaning |
| --- | --- | --- |
| `sequence` | integer | Journal sequence of the committed command. |
| `command` | string | Command name, such as `SET` or `DELETE`. |
| `key` | string | Affected key. |
| `state` | string | `committed` for entries returned by the durable journal tail. |

Mutation values are never returned. The default tail limit is
`DefaultSQLSystemMutationLimit` (`1000`) and the hard maximum is
`MaxSQLSystemMutationLimit` (`10000`). Set `MutationLimit` to a positive value
to choose a smaller bound; values above the hard maximum are clamped.

```sql
FROM CACHE('system.mutations')
SELECT sequence, command, key, state
ORDER BY sequence DESC
LIMIT 20
```

An absent journal produces an empty table. Journal I/O errors are returned to
the SQL caller.

### `system.queries`

This table contains currently active queries from the configured
`hatSql.SQLQueryManager`:

| Column | Type | Meaning |
| --- | --- | --- |
| `query_id` | string | Query-manager identifier. |
| `state` | string | Current query state. |
| `started_at` | time | Query start time. |
| `active` | bool | `true` in this table. |
| `cancel_reason` | string | Present when cancellation was requested. |
| `error_code` | string | Present when an execution error has a code. |

`finished_at` is omitted for active queries. Query text, source values,
parameters, and result rows are not exposed.

```sql
FROM CACHE('system.queries')
SELECT query_id, state, started_at
ORDER BY started_at
```

### `system.query_history`

This table contains the query manager's bounded completed-query history. It
uses the same columns as `system.queries`, with `active = false` and
`finished_at` present when completion has been recorded.

```sql
FROM CACHE('system.query_history')
SELECT query_id, state, started_at, finished_at, error_code
ORDER BY finished_at DESC
LIMIT 50
```

The history remains subject to the query manager's configured retention and
does not retain SQL text through this system-table adapter. Missing query
manager configuration produces empty `system.queries` and
`system.query_history` tables.

## Source names and compatibility

The system names are recognized case-insensitively only when the SQL source
name is `CACHE` and the key is one of the four names above. Every other source
is delegated to the wrapped resolver unchanged. Existing applications can
therefore adopt the catalog incrementally by wrapping their current resolver.

The adapter is observational: it does not lock the trie, journal, or query
manager while constructing rows. Treat a result as a point-in-time diagnostic
view rather than a transactionally consistent cross-system snapshot.

## Verification

The focused regression coverage is in
`hat/hatCache/system_tables_test.go`. It checks unpartitioned and partitioned
parts, journal mutations, active and completed query status, SQL execution over
the system source, delegation, the mutation bound, and privacy of values and
query text.
