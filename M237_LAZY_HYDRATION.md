# M237 Lazy Hydration

M237 adds `hatSql.SQLLazyHydrationRegistry` for maintained views and indexes
that can be rebuilt on first read instead of eagerly hydrating every object.

## Behavior

- `Register` adds one validated maintained object and a hydration callback.
- `Ensure` hydrates only the requested object when it is not ready.
- Concurrent readers of the same object share one callback; duplicate work is
  not started.
- A waiting reader can cancel its own wait without canceling the owner callback.
- The hydration callback receives the caller's context, so the owner can stop
  expensive work on cancellation.
- `Invalidate` clears readiness; the next reader hydrates again. If
  invalidation races with an active hydration, that successful result is not
  admitted as ready.
- A failed hydration is retryable by the next reader. `Snapshot`, `Len`, and
  `Unregister` provide lifecycle inspection and cleanup.

The registry does not store query results or publish table/index state itself.
The callback owns the rebuild and publication transaction, which keeps
durability and retry policy explicit.

## Benchmark

Workload: 8,192 registered objects and no-op hydrators, five 200 ms samples on
AMD Ryzen 9 5950X, `linux/amd64`.

| Path | Median ns/op | B/op | Allocs/op | Speed vs eager all |
| --- | ---: | ---: | ---: | ---: |
| Eagerly hydrate all 8,192 | 28,283 | 0 | 0 | 1.00x |
| Invalidate and lazily hydrate one object | 178.6 | 128 | 2 | 158.4x |
| Ready read, no hydration | 28.72 | 0 | 0 | 984.8x |

The cold lazy path includes invalidation and the first-reader lookup. Its
small allocation cost is proportional to the requested object's detached
metadata, not the catalog size. Ready reads are lock-protected and allocation
free. Raw samples are in [M237_BENCHMARK_RAW.txt](M237_BENCHMARK_RAW.txt).
