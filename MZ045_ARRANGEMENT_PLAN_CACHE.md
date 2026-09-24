# MZ-045 Arrangement Plan Cache

MZ-045 is the Materialize-inspired idea of reusing equivalent workload plans
instead of repeatedly resolving the same physical arrangement metadata. The
existing compiled-query cache avoids parsing, while this addition avoids
rebuilding scan/join arrangement recommendations during repeated `EXPLAIN`
calls.

## Behavior

`SQLArrangementPlanCache` is opt-in through `SQLQueryOptions`:

```go
cache, err := hatSql.NewSQLArrangementPlanCache(hatSql.SQLArrangementPlanCacheOptions{
	MaxEntries: 256,
	MaxBytes:   8 << 20,
})
if err != nil {
	return err
}

options := hatSql.SQLQueryOptions{
	ArrangementPlanCache:        cache,
	ArrangementPlanCacheVersion: "orders-schema-v7",
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, "EXPLAIN FROM CACHE('orders') SELECT id", resolver, options)
```

The default is off. A nil cache or an empty version bypasses the cache and
keeps the existing resolver path. The default limits are 64 entries and 1 MiB.
The cache uses an LRU eviction policy and exposes `Stats`, `Invalidate`, and
`InvalidateVersion` for operational inspection and maintenance.

Each entry is keyed by:

1. The compiled query token fingerprint.
2. The source kind and source key.
3. The caller-supplied arrangement metadata version.

When a source's arrangement definitions, indexes, schema, or physical layout
changes, advance `ArrangementPlanCacheVersion` or explicitly invalidate the
old version. The version is required so stale recommendations are never reused
under a new physical metadata generation. Cached metadata is cloned on output
to prevent caller mutation from changing retained entries.

This feature only reuses EXPLAIN metadata and recommendation decisions. It does
not create arrangements, change query execution, or automatically rewrite a
physical plan. Automatic physical planner rewrites remain future work.

## Measurements

The benchmark uses one normalized query, one local metadata resolver, a warm
cache hit, five samples, and `-benchtime=100x` on Linux amd64 / AMD Ryzen 9
5950X. It measures the repeated EXPLAIN path, not network or disk latency.

| Case | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Metadata resolution baseline | 2,468 | 3,026 | 23 |
| Warm versioned plan cache | 1,955 | 2,768 | 17 |

That is `1.26x` faster, `8.5%` fewer bytes, and `26.1%` fewer allocations in
this workload. A cold miss still performs resolution and recommendation
marking, with bounded cache bookkeeping. Very low-reuse workloads can pay the
mutex/map/LRU overhead without receiving a hit; keeping the feature disabled by
default avoids that cost for existing callers.

The raw samples and command context are recorded in
[`BENCHMARK.md`](BENCHMARK.md#mz-045-arrangement-plan-cache).

## Verification

```text
make format-mz045-arrangement-cache
make test-mz045-arrangement-cache
make benchmark-mz045-arrangement-cache
make verify-mz045-arrangement-cache
```

Coverage includes equivalent-query reuse, explicit version separation, cloned
metadata isolation, public `SQLQueryOptions` wiring, the full `hat/hatSql`
package, focused race testing, and `go vet`.
