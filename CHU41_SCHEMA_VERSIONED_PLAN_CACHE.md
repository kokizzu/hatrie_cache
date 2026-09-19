# CH-U41 Schema-Versioned Plan Cache

`hatSql.SQLPreparedQueryCache` can persist parsed SQL templates for restart
warming without putting persistence work on query execution. The feature is
opt-in.

## Usage

```go
cache := hatSql.NewSQLPreparedQueryCache(256)
_, err := hatSql.PrepareSQLQueryWithSchemaVersion(
    "SELECT value FROM CACHE('users') WHERE id = $1",
    []hatSql.ParameterSpec{{Type: hatSql.ParameterInteger}},
    "users-schema-v7",
    cache,
)
if err != nil {
    return err
}

if err := cache.Save("/var/lib/hatrie/prepared.cache", hatSql.SQLPreparedQueryCachePersistenceOptions{}); err != nil {
    return err
}

report, err := cache.Load("/var/lib/hatrie/prepared.cache", hatSql.SQLPreparedQueryCachePersistenceOptions{
    SchemaVersion: "users-schema-v7",
})
if err != nil {
    return err
}
_ = report.Loaded
```

The caller owns the schema dependency token. Change it whenever a source
schema, index, projection, or planner contract that can affect the query
changes. Restore admits only an exact token. Entries with an empty token are
skipped unless `AllowUnversioned` is explicitly true.

`InvalidateSchemaVersion` removes one dependency namespace. `Invalidate`
removes all in-memory plans. Loading validates and parses the complete file
before invalidating the selected namespace, so a corrupt file cannot leave a
partial restore.

## Format And Limits

The snapshot is a versioned binary envelope with length-prefixed UTF-8 source
and schema strings, plus a CRC-32 checksum. Writes use a same-directory
temporary file, `0600` permissions, `fsync`, and atomic rename. The snapshot
contains no bound parameter values or caller secrets. Parameter types remain
owned by each `SQLPreparedQuery`; after restore, a caller supplies and validates
the parameter schema again.

Defaults are 256 records and 8 MiB. Hard limits are 4,096 records, 64 MiB per
file, 1 MiB per SQL source, and 256 bytes per schema token. The newest LRU
records are retained when the snapshot entry bound is smaller than the cache.

## Measured Tradeoff

Five `-benchmem` samples were run through `make benchmark-chu41` on
Linux/amd64 with an AMD Ryzen 9 5950X. The baseline was run from clean `HEAD`
before the implementation.

| Operation | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Before: versioned cache hit | 31.40 | 0 | 0 |
| After: versioned cache hit | 27.73 | 0 | 0 |
| After: no-persistence hit control | 31.07 | 0 | 0 |
| After: save 256 plans | 1,013,948 | 63,692 | 18 |
| After: load 256 plans | 1,811,622 | 1,466,869 | 7,980 |

Save/load are maintenance operations and are not comparable to a single cache
hit. The important hot-path result is unchanged zero allocation; restart
warming trades bounded startup CPU and temporary heap for avoiding the first
parse on each restored plan. The load cost is intentionally visible and
bounded rather than hidden in query execution.
