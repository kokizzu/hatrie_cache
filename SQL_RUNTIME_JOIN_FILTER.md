# SQL Runtime Join Filter

Hatrie SQL has an opt-in ClickHouse-style runtime filter for one narrow query
shape: a direct two-source `CACHE` `INNER JOIN` on one equality field, with
simple field projections. The executor streams the right source once, builds an
exact hash table plus a compact Bloom filter of right-side keys, and uses the
Bloom filter before probing the exact table for each left-side row.

```go
options := hatSql.SQLQueryOptions{
	RuntimeJoinBloomFilter: true,
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, options)
```

The flag is disabled by default. The normal executor remains authoritative for
`LEFT`, `RIGHT`, and `FULL` joins, predicates, aggregates, ordering, limits,
subqueries, unions, typed sources, worker-parallel queries, join spilling, and
queries with an available equality index. A resolver must implement
`hatSql.StreamSQLSourceResolver`; otherwise the query falls back without a
runtime-filter plan step.

Bloom false positives only cause an exact hash-table probe. False negatives are
not possible for keys inserted into the filter, and SQL `NULL` join keys remain
non-matching. Duplicate right-side keys are retained and produce the same
many-to-many result as the established hash join.

## Tradeoff

The feature is useful when the probe side is much larger than the distinct
right-side key set. It allocates less because rejected left rows never become
join envelopes or result rows. A balanced join with mostly matching keys can be
slightly slower and allocate more because it pays for the Bloom filter and
streaming callbacks. That is why this is an explicit query option rather than a
new default.

Measured with `make benchmark-sql-runtime-join-filter`, five samples per path,
Linux on an AMD Ryzen 9 5950X. The reported values are medians; the benchmark
uses 100,000 left rows and 512 right rows for the selective case, 1,024 rows on
each side for the balanced case, and 100,000 left rows with one right row for
the hot-key case.

| Workload | Baseline time | Runtime-filter time | Time result | Baseline heap | Runtime-filter heap | Heap result | Baseline allocs | Runtime-filter allocs | Allocation result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Selective | 30.079 ms | 10.373 ms | 2.90x faster | 48.13 MB | 3.44 MB | 14.01x lower | 305,685 | 107,239 | 2.85x fewer |
| Balanced | 1.488 ms | 1.632 ms | 1.10x slower | 2.290 MB | 2.105 MB | 1.09x lower | 14,392 | 15,441 | 1.07x more |
| Hot key | 116.845 ms | 86.046 ms | 1.36x faster | 194.77 MB | 124.50 MB | 1.56x lower | 1,000,072 | 1,100,072 | 1.10x more |

The runtime filter therefore has a clear win for selective and hot-key joins,
but it is not a universal replacement for the existing hash path. Use the
option when the workload has a selective build-side key set and validate it
with the benchmark shape closest to production.
