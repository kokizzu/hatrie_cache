# M214 Cross-Plan Arrangement Metadata Reuse

M214 extends the existing bounded `SQLArrangementPlanCache` so compatible SQL
plans over the same source and metadata version share one resolver-owned
arrangement catalog lookup. The cache stores raw arrangement metadata and
applies workload-specific recommendation flags to a defensive copy for each
query, so one query cannot leak its recommendation into another.

```go
cache, err := hatSql.NewSQLArrangementPlanCache(hatSql.SQLArrangementPlanCacheOptions{})
if err != nil {
    return err
}
result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil,
    hatSql.SQLQueryOptions{
        ArrangementPlanCache:        cache,
        ArrangementPlanCacheVersion: "events-schema-v3",
    })
```

The feature is opt-in and remains metadata-only. It does not create, hydrate,
mutate, or release typed-table arrangements, and it does not alter ordinary
query results. Callers must advance `ArrangementPlanCacheVersion` or call
`Invalidate` when physical arrangement metadata changes. The bounded defaults
remain 64 entries and 1 MiB.

## Measurement

Linux `amd64`, AMD Ryzen 9 5950X, `go test -benchmem -count=5 -benchtime=500ms`.
The M214 workload creates two different `EXPLAIN` plans over the same source
and version in each iteration.

| Path | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Before: query-keyed cache | 15,046 | 7,434 | 63 | 1.00x |
| After: source/version reuse | 9,732 | 6,800 | 54 | 1.55x faster |

The change is a narrow improvement to the opt-in EXPLAIN metadata path. It
does not claim planner-wide reuse of live arrangement state; that broader
operation still requires a resolver-owned arrangement lifecycle. Because the
cache now stores raw metadata and reapplies recommendation flags per query, an
exact same-query cache hit costs about 64 additional B/op and four additional
allocations in the companion MZ045 control benchmark; the cross-plan workload
still wins materially because compatible plans share one resolver lookup.
