# CHG14 Prepared-Plan Cache Admission And Eviction Metrics

The normalized SQL prepared-query cache already exposed entries, hits, and
misses. This feature adds two counters needed to operate a bounded LRU cache:

- `Admissions` counts newly retained parsed templates.
- `Evictions` counts templates removed by LRU capacity pressure.

The counters are available from `SQLPreparedQueryCache.Stats()` and the
backward-compatible `PreparedQueryCacheStats` alias. A cache hit, including a
normalized alias hit, does not increment `Admissions`. Explicit invalidation
does not count as an eviction because it is operator-directed rather than
capacity pressure. A cache with non-positive capacity parses normally and
reports a zero metrics snapshot.

The counters retain no SQL text, parameter values, keys, or result rows. They
are protected by the cache's existing mutex. The successful hit path still
performs the same lookup and LRU move; only the existing miss/insert path
increments the new counters.

## Measurement

Machine: AMD Ryzen 9 5950X, linux/amd64. Each row is five `-benchmem` samples
from the same benchmark command.

| Workload | Before raw ns/op | Before median | After raw ns/op | After median | B/op | Allocs/op |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| Exact cache hit | 26.81, 25.89, 26.45, 28.09, 24.36 | 26.45 | 26.74, 26.49, 25.42, 25.76, 25.54 | 25.76 | 0 | 0 |
| Normalized alias hit | 14.60, 12.76, 13.04, 13.36, 13.31 | 13.31 | 12.92, 14.05, 13.47, 12.95, 13.39 | 13.39 | 0 | 0 |
| Versioned cache hit | 23.74, 22.53, 24.13, 24.50, 24.55 | 24.13 | 30.02, 24.47, 23.64, 24.73, 24.49 | 24.49 | 0 | 0 |

The exact-hit median was 1.03x faster after the change; normalized aliases
were 1.01x slower and versioned hits were 1.02x slower. Those differences are
within short benchmark noise, with no allocation or heap regression. The
feature is accepted for its operational value, not as a CPU optimization.

## Verification

- `make test-chg14`
- `make full-test-chg14`
- `make race-chg14`
- `make vet-chg14`
- `make benchmark-chg14-baseline`
- `make benchmark-chg14`
