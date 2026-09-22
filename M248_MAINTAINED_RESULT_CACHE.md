# M248 Maintained Result Cache

M248 adds an opt-in result cache for concurrent identical read expressions.
When several callers request the same cache key while its result is missing,
one caller executes the read and the other callers wait for that result instead
of duplicating the work.

## Usage

~~~go
cache := hatSql.NewMaintainedResultCacheWithOptions(
	hatSql.MaintainedResultCacheOptions{
		Capacity:    256,
		MaxInFlight: 64,
	},
)

result, err := hatSql.ExecuteSQLQuery(
	ctx,
	source,
	resolver,
	hatSql.SQLQueryOptions{ResultCache: cache},
)
~~~

NewMaintainedResultCache(capacity) uses the default MaxInFlight of 64.
NewMaintainedResultCacheWithDependencies(capacity) additionally enables
ExecuteWithDependencies and mutation-driven invalidation. Existing
NewResultCache and NewSQLResultCache constructors remain non-coalescing,
so the behavior is default-off.

MaxInFlight bounds distinct keys that may be waiting for an owner. Once the
bound is full, a new key executes independently and is not retained as an
in-flight entry. Retained results are still cloned before they are returned.

## Correctness

- The owner checks the source version before and after execution.
- If the source changes while the owner executes, waiters retry instead of
  receiving a result that failed the existing freshness contract.
- Executor errors are returned to all callers joined to that execution and
  are never retained.
- A waiting caller may cancel its own context without cancelling the owner.
- Dependency invalidation increments the existing generation fence; an owner
  that races with invalidation does not publish its result.

## Tradeoff

This is intended for expensive reads that are likely to arrive concurrently.
The maintained path adds a bounded in-flight map and one owner record per
miss. On the measured long-lived-cache benchmark, an ordinary versioned miss
was about 0.45 us, 360 B, and 4 allocations; the maintained miss was about
0.73 us, 648 B, and 6 allocations. That overhead is why the feature is
explicitly opt-in.

For eight callers of the same read with a serialized executor, the ordinary
cache executed eight misses in about 103-110 us per batch. The maintained
cache executed one miss in about 38-40 us, approximately 2.8x faster and 8x
fewer executor invocations. The batch allocation count increased from about
4.05 KB/50 allocations to 4.68 KB/54 allocations because the benchmark
creates the cache and caller goroutines per batch.

The measured win therefore applies when duplicate work is materially more
expensive than the coordination overhead. Existing caches have no in-flight
allocation or coalescing branch.

## Verification

~~~text
make format-m248
make test-m248
make race-m248
make vet-m248
make benchmark-m248
make benchmark-m248-serialized
~~~

The focused tests cover successful coalescing, shared executor errors, typed
result reuse, and dependency invalidation.
