# CH-008 Column TTL

`TypedTableColumn.TTL` is an opt-in policy for expiring a wide column without
removing its row. The zero value is disabled, so existing tables keep their
current storage and read paths.

## Configure

Processing-time TTL starts when `Upsert` accepts the row:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    Columns: []hatSql.TypedTableColumn{
        {Name: "id", Kind: hatSql.TypedTableInt64},
        {
            Name: "payload",
            Kind: hatSql.TypedTableString,
            TTL: hatSql.TypedTableTTLOptions{
                Mode:     hatSql.TypedTableTTLProcessingTime,
                Lifetime: 24 * time.Hour,
            },
        },
    },
})
```

Event-time TTL uses an int64 Unix-nanosecond column:

```go
{
    Name: "payload",
    Kind: hatSql.TypedTableString,
    TTL: hatSql.TypedTableTTLOptions{
        Mode:     hatSql.TypedTableTTLEventTime,
        Field:    "event_at",
        Lifetime: time.Hour,
    },
}
```

The event-time field must exist and have `TypedTableInt64` kind. Processing-time
TTL requires an empty `Field`. A caller-provided `Clock` makes tests and
recovery deterministic.

## Read and purge behavior

Before physical purge, an expired column is returned as SQL `NULL` while the
row remains visible. This applies to `Rows`, the SQL columnar resolver,
`Stats`, and `Histogram`; row cardinality and unrelated columns are unchanged.

`PurgeExpiredColumns(now)` clears expired values in storage and returns ordinary
`UPDATE` changes whose `Before` row contains the value and whose `After` row
contains `NULL`. This releases string/dictionary backing storage and gives
changefeed consumers an exact transition. The operation is idempotent.

`TypedTableTTLScheduler` remains opt-in. When registered, each pass runs both
row TTL and column TTL maintenance. `TypedTableTTLRun.ExpiredColumns` reports
rows physically cleared by column TTL; `Expired` continues to count row
deletions.

Column TTL does not change historical MVCC snapshot values merely because the
wall clock advances. Run a purge when the expiration must become a durable
change visible to historical/changefeed consumers.

## Backup and restore

For processing-time column TTL, call `MarshalColumnTTLState()` after backing up
rows and call `RestoreColumnTTLState()` after restoring the same rows and before
starting maintenance. The format is deterministic, bounded by
`MaxTypedTableTTLStateBytes`, CRC-protected, and matches rows by key and columns
by name. It rejects mismatched schemas, lifetimes, duplicate keys, missing
rows, and corruption without partially installing state.

Event-time column TTL stores no deadline state; restoring the timestamp column
and schema is sufficient. Row TTL continues to use its existing
`MarshalTTLState` and `RestoreTTLState` pair.

## Cost and scope

When no column TTL is configured, `columnTTLs` remains nil and the existing
row/read fast paths are retained. An enabled column TTL adds one int64 deadline
per processing-time-TTL column and checks configured columns during dynamic
reads. Event-time TTL adds no per-row deadline array. Physical purge is an
explicit mutation and therefore adds changefeed entries and invalidates derived
caches.

The implementation intentionally does not add automatic SQL schema inference or
background goroutines. Use the existing scheduler when automatic maintenance is
wanted.
