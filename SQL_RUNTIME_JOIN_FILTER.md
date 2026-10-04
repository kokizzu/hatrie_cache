# SQL Runtime Join Filter

Hatrie SQL has an opt-in ClickHouse-style runtime filter for a direct two-source
`CACHE` `INNER JOIN` on one equality field. The executor streams the right
source once, builds an exact hash table plus a compact Bloom filter of
right-side keys, and uses the Bloom filter before probing the exact table for
each left-side row. Non-subquery `WHERE` predicates and scalar projection
expressions are evaluated after exact matching in the same streaming path.

```go
options := hatSql.SQLQueryOptions{
	RuntimeJoinBloomFilter: true,
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, options)
```

The flag is disabled by default. The normal executor remains authoritative for
`LEFT`, `RIGHT`, and `FULL` joins, `HAVING`, aggregates, grouping, ordering,
limits, subqueries, unions, typed sources, worker-parallel queries, join
spilling, and queries with an available equality index. A resolver must implement
`hatSql.StreamSQLSourceResolver`; otherwise the query falls back without a
runtime-filter plan step.

Bloom false positives only cause an exact hash-table probe. False negatives are
not possible for keys inserted into the filter, and SQL `NULL` join keys remain
non-matching. Duplicate right-side keys are retained and produce the same
many-to-many result as the established hash join.

The post-join predicate is evaluated only after the exact hash bucket is found.
This keeps predicates that reference either input relation correct and avoids
turning a Bloom false positive into a result change. The default executor is
still used when the query shape is outside this bounded extension.

## Tradeoff

The feature is useful when the probe side is much larger than the distinct
right-side key set. It allocates less because rejected left rows never become
join envelopes or result rows. A balanced join with mostly matching keys can be
slightly slower and allocate more because it pays for the Bloom filter and
streaming callbacks. That is why this is an explicit query option rather than a
new default.

Measured with `make benchmark-chu14-runtime-filter`, five samples per path,
Linux on an AMD Ryzen 9 5950X. The reported values are medians; the benchmark
uses 100,000 left rows and 512 right rows for the selective case, 1,024 rows on
each side for the balanced case, and 100,000 left rows with one right row for
the hot-key case.

| Workload | Baseline time | Runtime-filter time | Time result | Baseline heap | Runtime-filter heap | Heap result | Baseline allocs | Runtime-filter allocs | Allocation result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Selective | 35.922 ms | 12.396 ms | 2.90x faster | 48.94 MB | 3.47 MB | 14.12x lower | 305,693 | 107,242 | 2.85x fewer |
| Balanced | 1.672 ms | 1.706 ms | 1.02x slower | 2.463 MB | 2.151 MB | 1.15x lower | 14,393 | 15,441 | 1.07x more |
| Hot key | 136.270 ms | 94.575 ms | 1.44x faster | 195.94 MB | 124.51 MB | 1.57x lower | 1,000,100 | 1,100,091 | 1.10x more |

The newly covered `WHERE` plus computed-projection shape was measured with
100,000 left rows, 512 right rows, and 511 output rows:

| Workload | Baseline time | Runtime-filter time | Time result | Baseline heap | Runtime-filter heap | Heap result | Baseline allocs | Runtime-filter allocs | Allocation result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Selective `WHERE` and projection | 43.856 ms | 11.598 ms | 3.78x faster | 48.93 MB | 3.47 MB | 14.10x lower | 305,953 | 107,508 | 2.85x fewer |

The runtime filter therefore has a clear win for selective and hot-key joins,
but it is not a universal replacement for the existing hash path. Use the
option when the workload has a selective build-side key set and validate it
with the benchmark shape closest to production.
