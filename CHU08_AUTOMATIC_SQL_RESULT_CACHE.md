# CH-U08 Automatic SQL Result Cache

CH-U08 wires the existing version-validated SQL result cache to direct
`HatTrie` SQL query entry points. It is intentionally opt-in because cache
hits still parse the query and deep-clone the returned result for caller
ownership.

## Configuration

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

if err := trie.ConfigureSQLResultCache(hatCache.DefaultSQLResultCacheCapacity); err != nil {
	panic(err)
}

result, err := hatCache.ExecuteSQLQuery(
	"FROM CACHE('people') AS person GROUP BY person.team SELECT person.team, count() AS total",
	trie,
)
```

`ConfigureSQLResultCache(0)` disables the feature and releases the bounded
cache. Negative capacities return `ErrSQLResultCacheCapacityInvalid`. The
default capacity constant is `128` entries, but automatic caching is disabled
until a positive capacity is configured.

`SQLResultCacheStats()` reports entries, hits, misses, bypasses, and evictions.
The configured cache is per trie and in-memory only. Reconfiguring a positive
capacity starts a fresh cache.

## Correctness Boundary

The `ExecuteSQLQuery`, `ExecuteSQLQueryContext`, and
`ExecuteSQLQueryParameters` wrappers attach the configured cache only when the
caller did not provide an explicit `SQLQueryOptions.ResultCache`. Existing
explicit caches continue to work unchanged. Streaming row, page, and keyset
page APIs do not use automatic result caching because their cursor and visitor
contracts are different.

`HatTrie.SQLSourceVersion` uses the trie mutation epoch. Any write therefore
invalidates all automatic cached results, including results for unrelated
keys. This is conservative but safe for the existing partition and resolver
implementation. The normal SQL eligibility checks still reject volatile,
explain, bounded-budget, historical, and custom-function queries.

The cache stores no disk or network data and does not expose query text or
result values through monitoring. It is subject to the same in-memory
retention and caller-trust considerations as the explicit result-cache API.

## Benchmark

Command:

```text
make benchmark-chu08-c248
```

Five runs on an AMD Ryzen 9 5950X, Linux amd64:

| Workload | CPU range | Bytes/op | Allocs/op | Relative result |
|---|---:|---:|---:|---|
| One-row direct uncached | 5.75-6.03 us | 4,624 | 23 | baseline |
| One-row automatic cache disabled | 5.75-6.30 us | 4,624 | 23 | no measurable default overhead |
| One-row automatic cache hit | 6.41-7.39 us | 6,056 | 42 | 1.06-1.28x slower, 1.31x bytes, 1.83x allocs |
| 20,000-row / 20-group direct uncached | 4.01-4.27 ms | 440,895-445,517 | 22,314-22,441 | baseline |
| 20,000-row / 20-group automatic cache hit | 11.97-12.63 us | 13,360 | 83 | 317-357x faster, about 33x lower bytes, about 270x fewer allocs |

The feature is therefore useful for repeated expensive analytical reads, not
for tiny reads. The default remains off so applications do not pay cache
retention or small-hit clone costs unless they choose the workload tradeoff.
