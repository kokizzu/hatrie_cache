# C208: Result-Cache Metrics

The SQL result cache now exposes cumulative hit, miss, bypass, and eviction counters through `ResultCache.Stats()` and the compatible `SQLResultCacheStats` name.

```go
cache := hatSql.NewSQLResultCache(256)
result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil, hatSql.SQLQueryOptions{
	ResultCache: cache,
})
if err != nil {
	return err
}

stats := cache.Stats()
log.Printf("cache entries=%d hits=%d misses=%d bypasses=%d evictions=%d",
	stats.Entries, stats.Hits, stats.Misses, stats.Bypasses, stats.Evictions)
_ = result
```

`Hits` counts a matching version or epoch lookup. `Misses` counts enabled-cache lookups that had no reusable entry or found a stale entry. `Bypasses` counts disabled caches, unavailable or unstable source versions/epochs, ineligible SQL result-cache requests, and other requests that cannot be retained. `Evictions` counts entries removed by the bounded LRU capacity. Counters are cumulative for the cache lifetime; `Entries` is the current retained-entry count.

`RecordBypass` is available to adapters that reject a query before calling the cache. The SQL executor records ineligible result-cache requests automatically. Existing prepared-query and condition-selection cache behavior is unchanged.

## Compatibility and cost

The API is additive. The default query path remains unchanged when `ResultCache` is nil. Counters use atomic increments alongside the existing cache mutex, so they add no allocations.

Measured on an AMD Ryzen 9 5950X, Linux amd64, Go, five samples, `-benchtime=1s -benchmem`:

| Workload | Pre-change median | Final median | Final B/op | Final allocs/op |
|---|---:|---:|---:|---:|
| Cached SQL result hit | 226,139 ns/op | 212,937 ns/op | 360,994 | 2,091 |
| Uncached control | 697,555 ns/op | 687,092 ns/op | 1,240,875 | 6,174 |

The final run did not show a regression; the small throughput differences are within normal benchmark variance. The metrics implementation introduced no measured allocation or memory increase.

## Verification

```text
make verify-c208
make benchmark-c208
```
