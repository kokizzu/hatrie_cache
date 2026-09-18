# CH-011: Projection DDL

CH-011 adds a small, safe DDL surface for user-defined query projections on
`SQLSession`. It reuses the existing `MaterializedViews` snapshot registry
instead of creating a second storage or dependency implementation.

## Commands

```sql
CREATE PROJECTION top_events AS
FROM CACHE('events')
SELECT id, score
ORDER BY score DESC
LIMIT 32

-- After the source version advances:
REFRESH PROJECTION top_events

DROP PROJECTION top_events
```

The commands are executed through `SQLSession.Execute`. Projection names are
simple session object identifiers. Creation parses the query, derives its
`CACHE` dependencies, requires every dependency to implement
`SourceVersionResolver`, and materializes the initial immutable snapshot.

## Read Semantics

The session automatically attaches its projection catalog to ordinary queries.
An exact query-text match is eligible for a hit only when every recorded source
version is still equal. A hit returns an independent result copy and reports a
`PROJECTION HIT` explain step. If a source version changes, the stale snapshot
is never returned; the query falls back to the normal source path. `REFRESH
PROJECTION` rebuilds the snapshot through the existing dependency-aware refresh
boundary, after which the same query can hit again.

This is deliberately explicit rather than a background watcher. A generic
`SourceResolver` has no reliable mutation notification, so callers should run
`REFRESH PROJECTION` after a committed source batch. The version guard makes a
miss cheaper than serving incorrect data.

## Lifecycle and Limits

- `CREATE PROJECTION` is session-local and is not persisted or replicated.
- `DROP PROJECTION` removes the snapshot and releases its materialized row and
  byte accounting.
- Unversioned sources are rejected at creation rather than creating a catalog
  entry that can never be safely selected.
- Caller-supplied `QueryOptions.ProjectionCatalog` remains respected; the
  session catalog is only installed when that option is nil.
- SQL expression parsing, durable projection metadata, automatic source-write
  notifications, and cross-node projection coordination remain future work.

## Benchmark

Command:

```text
make benchmark-ch011-projection-ddl
```

The benchmark compares the same top-32 query over 4,096 rows with no
projection and with a prebuilt exact projection. Creation is outside the timer.
It uses five `-benchtime=100x` samples on Linux/amd64 with an AMD Ryzen 9
5950X.

| Path | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full scan and sort | 9,004,302 | 5,219,497 | 28,718 | 1.00x |
| Exact projection hit | 16,014 | 15,456 | 80 | 562x lower time, 338x lower heap, 359x fewer allocations |

Raw final samples:

```text
BenchmarkCH011ProjectionFullScan-32 100 8774358 ns/op 5219825 B/op 28718 allocs/op
BenchmarkCH011ProjectionFullScan-32 100 8892007 ns/op 5219510 B/op 28718 allocs/op
BenchmarkCH011ProjectionFullScan-32 100 10393557 ns/op 5219497 B/op 28718 allocs/op
BenchmarkCH011ProjectionFullScan-32 100 10692056 ns/op 5219421 B/op 28718 allocs/op
BenchmarkCH011ProjectionFullScan-32 100 9004302 ns/op 5219351 B/op 28718 allocs/op
BenchmarkCH011ProjectionHit-32 100 14958 ns/op 15456 B/op 80 allocs/op
BenchmarkCH011ProjectionHit-32 100 23075 ns/op 15506 B/op 80 allocs/op
BenchmarkCH011ProjectionHit-32 100 19921 ns/op 15456 B/op 80 allocs/op
BenchmarkCH011ProjectionHit-32 100 16014 ns/op 15456 B/op 80 allocs/op
BenchmarkCH011ProjectionHit-32 100 10031 ns/op 15456 B/op 80 allocs/op
```

The result is workload-dependent: projection creation and refresh consume
storage and maintenance work, while the measured gain applies to repeated
exact queries over a source much larger than the returned limit.
