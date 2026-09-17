# MZ-30 Frontier-Aware Lookup-Join Cache

Materialize-style lookup arrangements can reuse a point lookup while the
source frontier is unchanged. `hatSql.SQLLookupJoinCache` adds that behavior to
external equality lookup joins without changing the default executor.

```go
cache := hatSql.NewSQLLookupJoinCache(1024)
result, err := hatSql.ExecuteQueryParameters(
    ctx,
    `FROM CACHE('orders') AS o
     LEFT JOIN EXTERNAL('countries') AS c ON o.country = c.code
     SELECT o.id, c.name`,
    resolver,
    nil,
    hatSql.QueryOptions{LookupJoinCache: cache},
)
```

The resolver must implement both `LookupSourceResolver` and
`SQLSourceFrontierResolver`. The executor captures a ready frontier once per
lookup join. A cache miss checks that the frontier is still unchanged before
retaining candidates; a later frontier advances causes the old entry to miss
and be replaced. If the frontier is unavailable, unready, or returns an error,
the ordinary lookup path is used. This is fail-open and never makes cache
availability a query requirement.

`NewSQLLookupJoinCache` uses caller-selected capacity and conservative defaults
of 128 rows and 64 KiB per entry. Use
`NewSQLLookupJoinCacheWithOptions` to set `MaxRowsPerEntry` and
`MaxBytesPerEntry`; negative bounds are rejected. `Stats` reports hits, misses,
bypasses, evictions, and frontier invalidations. `Clear` is intended for source
replacement or resolver restart. The cache is process-local and is not
persisted or sent over the wire.

Only external point lookups are cached. The key includes source name, source
key, indexed field, and the typed SQL lookup value. Candidates are copied when
retained and treated as immutable by the executor. Complex joins, unsupported
lookup values, and all existing non-cache paths retain their prior behavior.

## Measurement

Command:

```text
make benchmark-mz030
```

Five samples, `GOMAXPROCS=1`, 512 source rows, two repeated country keys:

| Path | Median CPU | Lookup calls/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 1,070,681 ns | 513.0 | 1,076,048 | 7,724 |
| Frontier cache | 993,442 ns | 1.0 | 1,080,144 | 7,724 |
| Improvement | 1.078x faster | 513x fewer | +4,096 | neutral |

The local benchmark's resolver is deliberately cheap, so most query allocations
remain in row merging. In a remote dictionary or networked lookup resolver,
the important result is one lookup call instead of one call per left row. The
bounded retained candidate copy adds 4 KiB to this fixture and is opt-in; the
default `LookupJoinCache: nil` path is unchanged.
