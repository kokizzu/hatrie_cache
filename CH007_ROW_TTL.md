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
- `PurgeExpired(now)` uses a live min-heap of expiry deadlines to find due
  rows without scanning the table, then turns them into ordinary `DELETE`
  changes so downstream arrangements and materialized consumers can apply the
  same changefeed contract.
- No background goroutine is started. Schedule `PurgeExpired` from the
  application's maintenance loop when physical cleanup and delete delivery
  are required.
- Processing-time updates renew the deadline. Event-time updates use the new
  event timestamp.

The expiry index has one entry per currently expiring physical row and a
position sidecar, so deletes and updates keep it bounded rather than leaving
stale deadlines behind. Due changes are still returned in physical row order;
the heap only determines which candidates need inspection.

The returned changes are ordered by the table's physical row order. With patch
parts enabled, expiry first creates tombstones; call `CompactPatchParts` under
the normal maintenance policy when physical removal is desired. Compaction
moves the processing-time deadline sidecar together with each surviving row.

## Cost And Limits

Processing-time TTL retains one `int64` deadline per physical row plus the
expiry index. On the benchmark platform, the index retained 24 bytes per
row (16-byte heap entry plus an 8-byte position), in addition to the 8-byte
deadline. Event-time TTL avoids the deadline sidecar but retains the index for
rows with a valid event timestamp. TTL-enabled tables do not reuse
time-insensitive columnar layouts, statistics, or histogram caches, because
the current time can change their result without a row mutation. The ordinary
TTL-disabled paths remain unchanged.

Exact cardinality for TTL-enabled tables scans rows to account for elapsed
deadlines. This is deliberate: there is no approximate count or hidden
background scheduler. `PurgeExpired` is constant-time for the common no-op
case and proportional to due rows plus heap maintenance. The benchmark below
makes the speed/memory tradeoff explicit.

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

The expiry-index comparison below uses the paired clean-overlay targets
`make benchmark-ch007-ttl-before-c225` and `make benchmark-ch007-ttl-c225`.
The baseline is the pre-index implementation; the optimized run includes the
index. Medians are from five runs on the same AMD Ryzen 9 5950X Linux amd64
host and use 4,096 rows.

| Operation | Before | After | Relative result |
| --- | ---: | ---: | ---: |
| `PurgeExpired` no-op | 13,865 ns/op | 9.931 ns/op | 1,396x faster |
| `PurgeExpired` one due row | 51,970 ns/op | 1,203 ns/op | 43.2x faster |
| Processing-time `Upsert` | 634.3 ns/op | 639.5 ns/op | 0.99x throughput |
| Processing-time delete/reinsert | 913.7 ns/op | 953.5 ns/op | 0.96x throughput |
| Expiry index retained memory | 0 bytes | 98,304 bytes | 24 bytes/row |

The no-op and sparse purge paths kept zero extra call-time allocations; the
sparse benchmark retained the existing five allocations per operation. The
index is therefore a good fit when periodic purge work matters, but it is not
free memory: a workload that never calls `PurgeExpired` should keep TTL
disabled, and background scheduling plus durable processing-time deadline
recovery remain intentionally outside this feature.
