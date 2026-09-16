# C205: Subquery Result Cache

## Status

Implemented as an opt-in `hatSql` execution option. The zero value is
unchanged: `SubqueryResultCache == nil` does not cache derived queries, CTE
bodies, or UNION branches.

## Usage

```go
cache := hatSql.NewSQLResultCache(256)
result, err := hatSql.ExecuteSQLQueryParameters(
    ctx,
    "FROM (FROM CACHE('events') SELECT id) AS e SELECT e.id",
    resolver,
    nil,
    hatSql.SQLQueryOptions{SubqueryResultCache: cache},
)
```

The cache is bounded by the capacity passed to `NewSQLResultCache`. Each
execution result is cloned on insertion and retrieval, so mutating a returned
row cannot mutate a later cached result. The cache is caller-owned and can be
shared by concurrent queries.

## Eligibility And Invalidation

The feature caches only uncorrelated, read-only query fragments that already
fit the existing result-cache safety rules:

- non-correlated derived queries;
- non-recursive CTE bodies when they do not read another CTE;
- UNION, INTERSECT, and EXCEPT branches;
- built-in deterministic expressions and versioned `CACHE`/`EXTERNAL` sources;
- the bound positional parameters, collation, prepared schema version, plan
  snapshot mode, and settings fingerprint are included in the cache key.

An entry is used only while every referenced source implements
`SourceVersionResolver` and reports the same non-empty version. A source
version change therefore invalidates the fragment without flushing unrelated
entries. Missing versions, resolver errors, unsupported options, volatile
expressions (`NOW`, `RAND`, `UUID`, and related forms), custom functions,
recursive CTE branches, correlated scalar subqueries, and lateral subqueries
fall back to normal execution.

The option is deliberately separate from `SQLQueryOptions.ResultCache`:
`ResultCache` caches the complete top-level result, while
`SubqueryResultCache` reuses eligible internal fragments. Applications can
enable either or both with independent bounded caches.

## Measurement

The benchmark uses 512 source rows with an `id` and a nontrivial payload. It
wakes the prepared-query cache before timing, runs with `GOMAXPROCS=1`, and
uses five one-second samples:

```text
make benchmark-chu05-c205-baseline
make benchmark-chu05-c205
```

| Path | Median ns/op | Median B/op | Median allocs/op | Source reloads | Relative time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before C205 | 474,245 | 893,840 | 4,135 | 1 per operation | Reference |
| After, option off | 466,653 | 893,904 | 4,135 | 1 per operation | 0.98x |
| After, option on and warm | 239,466 | 451,528 | 2,100 | 0 after warmup | 0.51x |

The default path is within normal benchmark noise of the pre-C205 path, with
the same allocation count and only 64 additional bytes per operation in this
fixture. The warmed opt-in path is about **1.98x faster**, uses **1.98x less
heap**, and uses **1.97x fewer allocations** than the pre-C205 path. Compared
with the option-off path, it is about **1.95x faster** and uses about half the
bytes and allocations.

### Raw baseline samples

```text
BenchmarkSQLSubqueryResultCacheBaseline  2121  503223 ns/op  893840 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheBaseline  2125  544565 ns/op  893840 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheBaseline  2557  474245 ns/op  893840 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheBaseline  2565  468877 ns/op  893840 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheBaseline  2590  447997 ns/op  893840 B/op  4135 allocs/op
```

### Raw post-C205 samples

```text
BenchmarkSQLSubqueryResultCacheDisabled  2046  502759 ns/op  893904 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheDisabled  2614  441027 ns/op  893904 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheDisabled  2655  466653 ns/op  893904 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheDisabled  2445  463900 ns/op  893904 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheDisabled  2635  501448 ns/op  893904 B/op  4135 allocs/op
BenchmarkSQLSubqueryResultCacheEnabled   4611  234353 ns/op  451528 B/op  2100 allocs/op
BenchmarkSQLSubqueryResultCacheEnabled   4887  239466 ns/op  451528 B/op  2100 allocs/op
BenchmarkSQLSubqueryResultCacheEnabled   4959  276034 ns/op  451528 B/op  2100 allocs/op
BenchmarkSQLSubqueryResultCacheEnabled   3865  272297 ns/op  451528 B/op  2100 allocs/op
BenchmarkSQLSubqueryResultCacheEnabled   4783  234127 ns/op  451528 B/op  2100 allocs/op
```
