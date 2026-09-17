# CH-U40 Dependency-Aware Result Invalidation

This feature adds an opt-in source dependency index to the SQL result cache.
It is inspired by dependency-driven invalidation used by analytical caches and
incremental dataflow systems: a mutation can remove only results that depend
on the changed source instead of clearing the complete cache.

## Default Behavior

The existing constructors and calls are unchanged:

```go
cache := hatSql.NewSQLResultCache(256)
```

`NewResultCache` and `NewSQLResultCache` do not allocate a dependency index.
The ordinary `Execute` and `ExecuteVersioned` paths continue to use their
existing epoch or source-version checks.

## Version-Checked Dependency Tracking

Use this mode when callers want selective invalidation but still want source
versions to protect correctness:

```go
cache := hatSql.NewSQLResultCacheWithDependencies(256)
options := hatSql.SQLQueryOptions{ResultCache: cache}

_, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil, options)
cache.InvalidateDependency("CACHE", "events")
```

SQL entries retain their `CACHE` or `EXTERNAL` source dependencies. The normal
version callback still runs on reads, while `InvalidateDependency` immediately
removes affected entries and releases their retained result data.

## Explicit Invalidation Mode

For a resolver without source-version reads, or when the lookup path must avoid
version callbacks, enable the explicit contract:

```go
cache := hatSql.NewSQLResultCacheWithDependencies(256)
options := hatSql.SQLQueryOptions{
	ResultCache:                    cache,
	ResultCacheExplicitInvalidation: true,
}

_, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil, options)
// Call this before any read that could observe the mutation.
cache.InvalidateDependency("CACHE", "events")
```

Every mutation of a source used by the cache must invalidate that source. The
explicit mode is intentionally opt-in because the cache cannot infer writes
made outside the query executor. An in-flight explicit query is prevented from
retaining a result across an invalidation generation change.

The generic API is also available:

```go
dependencies := []hatSql.ResultCacheDependency{{Kind: "CACHE", Key: "events"}}
cache.ExecuteWithDependencies(ctx, "query-key", dependencies, execute)
cache.InvalidateDependencies(dependencies)
```

`ExecuteVersionedWithDependencies` records dependencies while retaining the
existing version safety check.

## Persistence

Ordinary snapshots keep the existing compact format. Snapshots containing
dependency metadata use persistence format version 2; format version 1
remains readable. Restoring a dependency-aware cache rebuilds its source index
before it becomes visible to callers.

## Tradeoffs

- Default caches have no dependency map or per-entry dependency slice.
- Opt-in tracking uses one small map entry per distinct source-to-query link and
  retains a bounded dependency slice per cached result.
- Invalidation is proportional to entries affected by the source, plus a
  bounded LRU order compaction; unrelated entries remain cached.
- Explicit mode removes version-read CPU, but its correctness depends on the
  caller's mutation notifications. Version-checked mode is the safer default.
- Dependency metadata is bounded by the existing persistence file quota and a
  65,536-dependency decode limit.

## Measurement

The reproducible benchmark is:

```text
make benchmark-chu40-dependency-invalidation
```

See the CH-U40 section in `BENCHMARK.md` for the raw five-run output and the
comparison against the unchanged default versioned path.
