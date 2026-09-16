# Persisted SQL Result Cache

CH-U09 is implemented as an explicit, opt-in warm-start cache for materialized
SQL results. It does not change the default query path and it is not a backup of
the underlying cache data.

## API

The portable API lives in `hat/hatSql`:

```go
cache := hatSql.NewSQLResultCache(128)
if err := cache.Restore("data/sql-result-cache.bin"); err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, params,
	hatSql.SQLQueryOptions{ResultCache: cache})
if err != nil {
	return err
}

if err := cache.Persist("data/sql-result-cache.bin"); err != nil {
	return err
}
```

`hatCache.SQLResultCache` exposes the same `Persist`, `Restore`, and
`WithOptions` methods. An automatic `HatTrie` cache can use
`ConfigureSQLResultCache`, `RestoreSQLResultCache`, and
`PersistSQLResultCache`.

Persistence remains disabled unless the caller creates or configures a result
cache. There is no background writer and no per-query filesystem write. A
missing file is a cold start.

## Validation And Recovery

Only entries created by `ExecuteVersioned` are persisted. Process-local numeric
epochs from the generic `Execute` API are excluded. Every entry retains its
exact cache key and non-empty source version; the key already contains the SQL
text, parameters, collation, prepared schema version, plan-snapshot choice, and
optional settings fingerprint. A changed source version or schema/settings key
therefore misses normally after restore.

The file has a magic/version header, payload length, SHA-256 checksum, and a
compact tagged binary payload. Writes use a `0600` temporary file, `fsync`, and
atomic rename. Restore validates the complete file and all value bounds before
replacing typed entries. Corruption or an unsupported payload returns
`ErrSQLResultCachePersistenceCorrupt` and leaves the live cache unchanged.
`SQLResultCachePersistenceOptions.MaxBytes` bounds both reads and writes; zero
uses the `64 MiB` default. The limit is a file quota, not a memory reservation.

Stored values preserve the SQL scalar types used by the result cache, including
integer widths, floating-point widths, `time.Time`, date/decimal/UUID/duration
aliases, IPv4/IPv6, bytes, arrays, and objects. Unsupported application-defined
value types fail `Persist` instead of being silently converted. The file is a
cache artifact containing query text and result values: keep it on a protected
filesystem and encrypt the directory or disk when those values are sensitive.

## Measured Tradeoff

`make benchmark-chu09-c249` uses 1,024 rows and five samples per benchmark on
Linux/amd64, AMD Ryzen 9 5950X. A memory hit is the ordinary in-process
baseline. Persist includes snapshot cloning, binary encoding, `fsync`, and
atomic replacement. Restore includes file read, checksum, decode, and owned
value reconstruction.

| Workload | Median CPU | Bytes/op | Allocs/op | Result |
|---|---:|---:|---:|---|
| In-memory hit | 0.38 ms | 394,578 B | 4,098 | lowest-latency steady state |
| Persist, compact binary | 2.45 ms | 1,505,842 B | 13,354 | about 6.4x memory-hit CPU |
| Restore, compact binary | 1.74 ms | 1,541,100 B | 35,648 | one-time cold-start cost |
| JSON encoding reference | 0.79 ms | 257,535 B | 2,050 | smaller CPU-only encoding, but loses dynamic SQL scalar types |

The compact binary snapshot is `88,098` bytes for this workload. The earlier
`gob` payload measured `115,546` bytes, so the final payload is `1.31x` smaller;
persist CPU was about `1.31x` faster at the median and restore about `1.25x`
faster. Restore allocates about `12%` more objects than the `gob` draft because
it validates and owns each decoded string/byte slice. The feature is useful
when a repeated expensive query costs more than the one-time restore, but it
is not appropriate for every small or latency-critical query.
