# SQL Runtime Join Filter

Hatrie SQL has an opt-in ClickHouse-style runtime filter for one narrow query
shape: a direct two-source `CACHE` `INNER JOIN` on one equality field, with
simple field projections and built-in `WHERE` predicates. The executor streams
the right source once, builds an exact hash table plus a compact Bloom filter
of right-side keys, and uses the Bloom filter before probing the exact table
for each left-side row.

```go
options := hatSql.SQLQueryOptions{
	RuntimeJoinBloomFilter: true,
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, options)
```

The flag is disabled by default. The normal executor remains authoritative for
`LEFT`, `RIGHT`, and `FULL` joins, custom-function predicates, aggregates,
ordering, limits, subqueries, unions, typed sources, worker-parallel queries,
join spilling, and queries with an available equality index. A resolver must implement
`hatSql.StreamSQLSourceResolver`; otherwise the query falls back without a
runtime-filter plan step.

Bloom false positives only cause an exact hash-table probe. False negatives are
not possible for keys inserted into the filter, and SQL `NULL` join keys remain
non-matching. Duplicate right-side keys are retained and produce the same
many-to-many result as the established hash join. A supported `WHERE`
expression is evaluated after exact candidate rows are merged, preserving
predicates that reference either side of the join and SQL NULL truth behavior.

## Tradeoff

The feature is useful when the probe side is much larger than the distinct
right-side key set. It allocates less because rejected left rows never become
join envelopes or result rows. A balanced join with mostly matching keys can be
slightly slower and allocate more because it pays for the Bloom filter and
streaming callbacks. That is why this is an explicit query option rather than a
new default.

Measured with `make benchmark-chu14-runtime-filter`, five samples per path,
Linux on an AMD Ryzen 9 5950X. The reported values are medians; the benchmark
uses 100,000 left rows and 512 right rows for the selective case, the same
shape with `WHERE l.id < 512`, 1,024 rows on each side for the balanced case,
and 100,000 left rows with one right row for the hot-key case.

| Workload | Baseline time | Runtime-filter time | Time result | Baseline heap | Runtime-filter heap | Heap result | Baseline allocs | Runtime-filter allocs | Allocation result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Selective | 47.189 ms | 15.062 ms | 3.13x faster | 48.935 MB | 3.466 MB | 14.11x lower | 305,697 | 107,242 | 2.85x fewer |
| Selective `WHERE` | 45.328 ms | 15.165 ms | 2.99x faster | 46.549 MB | 3.468 MB | 13.42x lower | 206,214 | 107,248 | 1.92x fewer |
| Balanced | 1.970 ms | 2.002 ms | 1.02x slower | 2.464 MB | 2.152 MB | 1.15x lower | 14,395 | 15,441 | 1.07x more |
| Hot key | 164.082 ms | 111.876 ms | 1.47x faster | 195.937 MB | 124.506 MB | 1.57x lower | 1,000,109 | 1,100,094 | 1.10x more |

Raw benchmark samples (`ns/op`, `B/op`, `allocs/op`):

```text
selective baseline: 47,768,298 46,564,645 46,972,551 47,189,418 47,835,523; 48,935,421 B/op median; 305,697 allocs/op median
selective runtime:  14,682,139 16,522,488 15,710,116 14,293,858 15,062,228;  3,466,351 B/op median; 107,242 allocs/op median
where baseline:     46,145,346 50,733,988 45,328,464 40,923,158 36,687,583; 46,549,114 B/op median; 206,214 allocs/op median
where runtime:      13,876,842 15,611,952 14,812,438 15,446,477 15,165,238;  3,467,791 B/op median; 107,248 allocs/op median
balanced baseline:   2,018,922  2,111,327  1,943,557  1,969,763  1,915,222;  2,463,928 B/op median;  14,395 allocs/op median
balanced runtime:    2,105,316  2,164,111  1,904,635  2,001,869  1,948,430;  2,151,536 B/op median;  15,441 allocs/op median
hot baseline:      217,520,055 164,081,702 172,596,261 148,750,424 160,248,168; 195,936,772 B/op median; 1,000,109 allocs/op median
hot runtime:       100,302,460 113,756,122 111,875,952 111,827,586 113,286,250; 124,506,108 B/op median; 1,100,094 allocs/op median
```

The runtime filter therefore has a clear win for selective and hot-key joins,
but it is not a universal replacement for the existing hash path. Use the
option when the workload has a selective build-side key set and validate it
with the benchmark shape closest to production.
