# CH-G14 Prepared-Plan Admission And Eviction Metrics

The prepared SQL cache now exposes bounded lifecycle counters through its
existing `Stats()` snapshot. This is a ClickHouse-inspired observability
addition: it does not change cache admission, LRU order, invalidation, query
results, or the default cache capacity.

```go
cache := hatSql.NewSQLPreparedQueryCache(256)
// Use the cache through the existing Parse/Prepare APIs.
stats := cache.Stats()
fmt.Println(stats.Hits, stats.Misses, stats.Admissions, stats.Evictions)
```

`Admissions` increments only after a valid parsed template is inserted into the
bounded cache. `Evictions` increments only when the capacity path removes the
least-recently-used entry. Cache hits, parse failures, explicit
`Invalidate`/`InvalidateSchemaVersion`, and a nonpositive capacity do not
increment either counter. This separation makes a high eviction rate
distinguishable from deliberate schema invalidation.

The counters are protected by the cache's existing mutex and are included in
the immutable `SQLPreparedQueryCacheStats` value. The implementation adds two
`uint64` fields, or 16 bytes per cache object, and no per-query allocation.

## Measurement

Five `-benchmem` samples were run through the Makefile targets on Linux/amd64
with an AMD Ryzen 9 5950X:

| Workload | Before median | After median | B/op before/after | Allocs/op before/after |
| --- | ---: | ---: | ---: | ---: |
| exact prepared-cache hit | 27.37 ns | 27.91 ns | 0 / 0 | 0 / 0 |
| normalized alias hit | 14.65 ns | 14.56 ns | 0 / 0 | 0 / 0 |
| schema-versioned hit | 25.60 ns | 25.50 ns | 0 / 0 | 0 / 0 |

The exact-hit sample is 1.02x slower in this run; the other two are 1.01x and
1.00x faster. These differences are within normal microbenchmark noise. The
feature is retained because it adds the requested operational signal without
changing the allocation-free hot path; the bounded 16-byte object cost is the
explicit tradeoff.

## Verification

```text
make test-chg14
make race-chg14
make vet-chg14
make benchmark-chg14-before
make benchmark-chg14-after
```

The focused tests cover hit/admission separation, LRU eviction, parse-failure
rejection, and invalidation not being counted as eviction.
