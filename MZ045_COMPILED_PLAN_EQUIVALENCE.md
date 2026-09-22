# Canonical Compiled-Plan Equivalence

`hatSql.SQLCompiledQueryCache` first checks the exact source string, then uses
the existing unambiguous token-normalized prepared-cache key on a miss. SQL
that differs only by whitespace or keyword casing shares one immutable
`CompiledSQLQuery`. Literal values, identifiers, and schema-version namespaces
remain distinct, so this does not merge queries with different results.

The cache stays bounded by its existing entry and byte limits. Canonical
entries share one LRU position, and eviction/invalidation removes both the
raw and canonical lookup paths. Exact-source hits do not lex or allocate.

This is a scoped MZ-045 adoption. It improves compiled-plan reuse and now also
provides the opt-in `SQLArrangementRecommendationCache` for literal-independent
arrangement choices. The cache key includes the source kind/key, a caller-owned
metadata version, and the normalized filter, grouping, ordering, join, and
locality workload. Empty versions bypass caching, so changed metadata cannot be
silently reused. The bounded LRU is concurrency-safe and never creates or
changes an arrangement; automatic planner wiring remains caller-owned.

Within one `EXPLAIN`, the normalized arrangement workload is still derived once
per query and reused for its scan and join steps. This avoids repeated shape
extraction without changing which arrangement is selected.

The recommendation cache was measured on the same three-candidate workload over
five samples. Direct selection used a median 642.5 ns/op, 88 B/op, and 4
allocations/op. A versioned cache hit used 197.3 ns/op, 0 B/op, and 0
allocations/op: 3.26x faster with 88 fewer bytes and four fewer allocations per
recommendation. The cache is opt-in and bounded, so this cost is paid only by
callers that request cross-call recommendation reuse.

See the benchmark details in [BENCHMARK.md](BENCHMARK.md#mz-045-canonical-compiled-plan-equivalence)
and [the EXPLAIN workload reuse result](BENCHMARK.md#mz-045-explain-workload-reuse).
