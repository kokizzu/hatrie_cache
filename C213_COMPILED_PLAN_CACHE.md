# C213: Bounded Compiled SQL Plan Cache

This feature adopts compiled-expression reuse ideas used by analytical and
in-memory databases without adding a machine-code JIT, unsafe code, or a new
runtime dependency. `CompiledSQLQuery` already owns an immutable parsed template
and lazily memoizes its native dataflow plan. `SQLCompiledQueryCache` reuses
those handles by exact SQL source and optional schema-version namespace.

## Default And API

Direct `hatSql` calls remain opt-in: existing calls do not allocate or consult
this cache. Use the package-native API when the same query source is compiled
repeatedly:

```go
cache, err := hatSql.NewSQLCompiledQueryCache(
	hatSql.DefaultSQLCompiledQueryCacheOptions(),
)
if err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryParameters(
	ctx,
	source,
	resolver,
	parameters,
	hatSql.SQLQueryOptions{CompiledCache: cache},
)
```

The default bounds are 64 entries and 8 MiB of estimated cache weight. Both
limits are enforced. Applications can pass `SQLCompiledQueryCacheOptions` with
smaller or larger positive limits. The constructor does not preallocate the
configured entry count, so a large limit does not immediately reserve a large
hash table.

`CompiledSQLQueryCache.Compile` and `CompileWithSchemaVersion` return immutable,
concurrency-safe handles. `CompiledSQLQueryCache.Stats` reports entries,
estimated bytes, hits, misses, evictions, and oversized plans. `Invalidate`
clears all entries; `InvalidateSchemaVersion` clears one schema namespace. A
schema or index rebuild should either change `PreparedSchemaVersion` or call
the corresponding invalidation method.

The byte limit is conservative accounting, not a process RSS measurement. The
current estimate is `4096 + 8*len(source)` bytes per retained source. It bounds
cache admission but cannot bound temporary allocations needed to compile an
oversized or malformed source. Oversized valid plans are returned to the caller
but are not retained.

The cache compiles outside its mutex and rechecks under the mutex before
inserting. Concurrent misses therefore converge on one retained handle, while
the loser compilation is discarded. Bound parameter values are never stored in
the cache. Static default-option executions use the immutable template directly;
parameterized or option-sensitive executions clone it before binding or rewrite.

## Benchmark

Five `-benchmem -cpu=1` samples on Linux/amd64, AMD Ryzen 9 5950X, measured
repeated compilation of the same short SQL source:

| Path | Median ns/op | B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Direct `CompileSQLQuery` | 3,839 | 6,080 | 19 | baseline |
| Bounded cache hit | 18.06 | 0 | 0 | 212.6x faster; 100% lower measured transient bytes; 100% fewer measured allocations |

Raw samples:

```text
Direct compile: 4103, 3842, 3839, 3729, 3777 ns/op; 6080 B/op; 19 allocs/op
Cache hit: 18.02, 18.18, 18.06, 18.06, 17.67 ns/op; 0 B/op; 0 allocs/op
```

This isolates compilation and cache lookup. It does not claim that query
execution is 212.6x faster. The retained plan is an intentional bounded memory
cost, and a cold miss still pays normal compilation plus cache metadata. Use
this for hot, repeated query sources; keep the default disabled for one-shot or
high-cardinality query text.

The full raw result is also recorded in
[`BENCHMARK.md`](BENCHMARK.md#bounded-compiled-sql-plan-cache).

### SQL Adapter Registry

`hatStorage.NewSQLAdapterRegistry` now supplies the same bounded cache to
queries executed through a namespace registry. This covers the common service
path without changing direct `hatSql` defaults:

```go
registry, err := hatStorage.NewSQLAdapterRegistry(nil, adapter)
if err != nil {
	return err
}

result, err := registry.Execute(ctx, "remote", source, parameters, hatSql.SQLQueryOptions{})
stats := registry.CompiledQueryCacheStats()
```

Use `NewSQLAdapterRegistryWithOptions` to inject a cache with a different
bound, or set `DisableCompiledCache: true` for a one-shot/high-cardinality
workload. A per-request `SQLQueryOptions.CompiledCache` still takes precedence.
The registry cache is bounded to 64 entries and 8 MiB by default and retains
only immutable query templates, never bound parameter values.

The registry benchmark used the same short resolver query on Linux/amd64 with
an AMD Ryzen 9 5950X and five `-benchmem` samples. The cached default reduced
both compile overhead and execution-path allocations; the explicit no-cache
variant stayed on the old behavior:

| Registry path | Median ns/op | B/op | Allocs/op | Improvement vs pre-change registry |
| --- | ---: | ---: | ---: | ---: |
| Before: no compiled cache | 6,095 | 4,832 | 21 | baseline |
| After: bounded cache default | 4,203 | 2,480 | 17 | 1.45x faster; 1.95x lower transient bytes; 1.24x fewer allocations |
| After: explicit `DisableCompiledCache` | 5,805 | 4,832 | 21 | within benchmark variance; no retained plan |

The cache retains bounded plan metadata, so its process memory is not zero even
though the hit path reports 2,480 transient bytes/op. `Stats().Bytes` is the
conservative cache accounting value, not RSS.
