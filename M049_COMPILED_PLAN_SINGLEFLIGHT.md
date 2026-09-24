# M049 Compiled Plan Miss Coalescing

## Why

`SQLCompiledQueryCache` already reuses completed immutable compiled plans, but
concurrent callers that miss the same exact source could all parse and compile
it before the final cache recheck. That amplified CPU, transient heap, and
garbage-collector pressure during query bursts.

This change adds exact-key singleflight behavior. The first caller becomes the
leader; concurrent callers for the same source and schema version wait for the
leader and receive the same immutable `*CompiledSQLQuery` or the same compile
error. Completed cache hits are unchanged. Canonically equivalent but
different source strings are intentionally not coalesced because they are not
the same exact in-flight key.

`SQLCompiledQueryCacheStats.Coalesced` reports the number of waiters served by
an in-flight compilation. It is diagnostic only and does not retain SQL text.

## Behavior And Tradeoff

- The feature applies only to callers using `SQLCompiledQueryCache`.
- `CompileSQLQuery` and all zero-value query execution defaults are unchanged.
- The cache stores one temporary flight record per concurrent exact-key miss;
  the record is removed before the call returns, whether compilation succeeds
  or fails.
- A failed compile is broadcast to current waiters and is not inserted into
  the completed cache.
- The cache still bounds retained completed plans by `MaxEntries` and
  `MaxBytes`; in-flight records are bounded by concurrent unique misses rather
  than those completed-plan limits.
- The optimization does not alter SQL parsing, execution, serialization,
  persistence, replication, authorization, or wire formats.

## Measurement

Command:

```text
make benchmark-m049-compiled-cache
```

The benchmark starts 16 concurrent callers against a fresh cache for each
iteration and uses five samples with `-benchmem` on Linux/amd64, AMD Ryzen 9
5950X.

| Metric | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| Median burst CPU | 94,921 ns/op | 48,738 ns/op | 1.95x faster |
| Median transient allocation | 47,550 B/op | 13,857 B/op | 3.43x lower |
| Median allocations | 192 allocs/op | 69 allocs/op | 2.78x fewer |

The benefit is specific to concurrent cold misses. A completed-cache hit does
not allocate a flight, and a single uncached caller still pays normal compile
cost plus one short-lived flight record.

## Verification

```text
make test-m049-compiled-cache
make test-m049-sql-package
make race-m049-sql-package
make vet-m049-sql-package
```

The focused tests cover same-pointer success fan-out, exact coalescing counts,
and compile-error fan-out. The full package, race detector, and vet checks
passed before delivery.
