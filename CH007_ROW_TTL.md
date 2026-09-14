# CH-007 Row TTL

`TypedTable` supports opt-in row expiration inspired by ClickHouse TTL
policies. The default is disabled, so existing tables retain their current
read, storage, and performance behavior.

## Configuration

Processing-time TTL starts the lifetime when `Upsert` accepts a row:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "sessions",
    TTL: hatSql.TypedTableTTLOptions{
        Mode:     hatSql.TypedTableTTLProcessingTime,
        Lifetime: 24 * time.Hour,
    },
    Columns: []hatSql.TypedTableColumn{
        {Name: "user", Kind: hatSql.TypedTableString},
    },
})
```

Event-time TTL derives the expiration time from an `int64` Unix-nanosecond
column:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    TTL: hatSql.TypedTableTTLOptions{
        Mode:     hatSql.TypedTableTTLEventTime,
        Field:    "event_time",
        Lifetime: 7 * 24 * time.Hour,
    },
    Columns: []hatSql.TypedTableColumn{
        {Name: "event_time", Kind: hatSql.TypedTableInt64},
        {Name: "payload", Kind: hatSql.TypedTableString},
    },
})
```

An event-time value is written with `TypedInt64(eventTime.UnixNano())`.
`NULL` event timestamps do not expire. `Clock` is optional and is intended for
deterministic tests or an application-owned time source.

## Expiration And Purging

Expiry is lazy and read-safe:

- `Rows`, SQL source resolution, columnar batches, cardinality, statistics,
  and histograms exclude expired rows immediately.
- `PurgeExpired(now)` turns expired rows into ordinary `DELETE` changes so
  downstream arrangements and materialized consumers can apply the same
  changefeed contract.
- No background goroutine is started. Schedule `PurgeExpired` from the
  application's maintenance loop when physical cleanup and delete delivery
  are required.
- Processing-time updates renew the deadline. Event-time updates use the new
  event timestamp.

The returned changes are ordered by the table's physical row order. With patch
parts enabled, expiry first creates tombstones; call `CompactPatchParts` under
the normal maintenance policy when physical removal is desired. Compaction
moves the processing-time deadline sidecar together with each surviving row.

## Cost And Limits

Processing-time TTL retains one `int64` deadline per physical row: 8 bytes per
row before allocator overhead. Event-time TTL retains no deadline sidecar but
reads the configured event column during visibility checks. TTL-enabled tables
do not reuse time-insensitive columnar layouts, statistics, or histogram
caches, because the current time can change their result without a row
mutation. The ordinary TTL-disabled paths remain unchanged.

Exact cardinality for TTL-enabled tables scans rows to account for elapsed
deadlines. This is deliberate: there is no approximate count or hidden
background scheduler. The benchmark below makes that tradeoff explicit.

## Backup And Recovery

The event-time policy is reconstructible from the persisted event-time column.
The processing-time deadline sidecar is runtime state and this feature does
not add it to a generic table serialization format. Rebuilding a processing-
time table by replaying rows therefore starts their TTL from the rebuild
`Upsert` time. Applications that require age to survive restart should persist
an event-time column and use event-time TTL, or persist and restore their own
processing-time metadata before enabling reads.

After recovery, call `PurgeExpired(recoveryNow)` to emit expiry changes and
physically clean rows according to the application's recovery policy.

## Benchmark

Command:

```text
make benchmark-ch007-row-ttl-c203
```

Machine: AMD Ryzen 9 5950X, Linux amd64. Five runs were collected over a
4,096-row, two-int64-column table; all rows were visible during the `Rows`
benchmark.

| Operation | Disabled | Processing time | Event time |
| --- | ---: | ---: | ---: |
| `Rows` median | 1.058 ms | 0.997 ms | 1.190 ms |
| `Rows` allocated bytes/op | 1,865,738 | 1,865,738 | 1,865,736 |
| `Rows` allocations/op | 20,225 | 20,225 | 20,225 |
| `SQLSourceCardinality` median | 15.36 ns | 12.832 us | 45.426 us |
| `SQLSourceCardinality` allocated bytes/op | 0 | 0 | 0 |
| TTL deadline sidecar | 0 bytes | 32,768 bytes | 0 bytes |

The `Rows` differences are within normal benchmark noise for this
allocation-dominated fixture. The exact-cardinality scan is approximately
835x slower for processing-time TTL and 2,958x slower for event-time TTL than
the unchanged O(1) disabled path; use `PurgeExpired` and ordinary TTL-disabled
tables when that planning hint must remain constant-time.
