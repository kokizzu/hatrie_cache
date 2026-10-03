# CH-G14 Prepared-Plan Cache Metrics

The prepared SQL cache now exposes bounded operational status through
`SQLPreparedQueryCache.Stats()`:

```go
cache := hatSql.NewSQLPreparedQueryCache(256)
stats := cache.Stats()
fmt.Printf("entries=%d/%d hits=%d misses=%d evictions=%d estimated_bytes=%d\n",
	stats.Entries,
	stats.Capacity,
	stats.Hits,
	stats.Misses,
	stats.Evictions,
	stats.EstimatedBytes,
)
```

The fields are:

| Field | Meaning |
| --- | --- |
| `Capacity` | Configured entry limit; non-positive cache capacity is reported as zero. |
| `Entries` | Current number of retained parsed templates. |
| `Hits` / `Misses` | Successful cache lookups and parsed-template admissions. |
| `Evictions` | Cumulative least-recently-used removals. It is not reset by invalidation. |
| `EstimatedBytes` | Cache-owned key-string bytes plus bounded entry overhead currently retained. |

`EstimatedBytes` is intentionally an accounting estimate, not a replacement for
process RSS: Go map/list overhead and the full recursive size of the parsed AST
are not measured. It is useful for bounded admission dashboards and comparing
cache generations. `Invalidate` and schema-version invalidation release the
accounted bytes while preserving hit, miss, and eviction counters.

The ordinary query path is unchanged. Cache hits only update the existing
counter and LRU position; disabled caches (`capacity <= 0`) do not retain
entries or update storage counters. The additional per-entry accounting field
is eight bytes before allocator/container overhead.

## Verification

```text
make test-chg14-prepared-cache
make benchmark-chg14-prepared-cache
make race-chg14-prepared-cache
make vet-chg14-prepared-cache
```

The implementation was tested first with a failing admission/eviction/memory
contract test, then with the full `hat/hatSql` package, race detection, and
vet. The focused benchmark uses five samples on Linux/amd64 with an AMD Ryzen
9 5950X:

| Workload | Before median | After median | B/op before/after | Allocs/op before/after |
| --- | ---: | ---: | ---: | ---: |
| Prepared-cache hit | 27.28 ns | 26.90 ns | 0 / 0 | 0 / 0 |
| Exact-key control | 26.01 ns | 25.75 ns | 0 / 0 | 0 / 0 |
| Capacity-1 eviction | 3,636 ns | 3,703 ns | 4,539 / 4,538 | 20 / 20 |

The small hit/control differences are noise-level and are not presented as a
performance win. The retained feature is an operational visibility gain with
no measured transient allocation or byte increase in the eviction workload.
Raw samples are recorded in `BENCHMARK.md`.
