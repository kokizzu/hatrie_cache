# CH-U14 Runtime Join Filter Propagation

This adopts a bounded ClickHouse-style runtime filter for one safe SQL shape:
an opt-in direct `INNER JOIN` between two `CACHE` sources on one equality key,
with direct field projections and no `WHERE`, grouping, ordering, limits,
subqueries, aggregates, windows, or index hint.

`QueryOptions.RuntimeJoinBloomFilter` remains `false` by default. When enabled,
the executor builds an exact right-side hash table and a compact Bloom filter
over distinct right keys. Probe keys that miss the Bloom filter are skipped;
every key that passes still uses the exact hash table, so false positives add
work but cannot change results. NULL keys remain non-matches under SQL inner
join semantics, and duplicate keys retain source order.

Resolvers that implement `SQLStreamSourceResolver` use the existing streaming
implementation. Legacy materialized resolvers now use the same runtime-filter
contract after resolving raw source slices, avoiding `sqlExecRow` wrappers for
unmatched probe rows. Context-aware materialized resolvers receive the query
context through `resolveSQLSourceContext`.

The fast path declines queries with join byte budgets, parallel workers,
unsupported clauses, right-side indexes, non-`CACHE` sources, outer joins, or
non-field projections. Those queries retain the established executor. This
keeps runtime filtering explicitly opt-in and limits the implementation to a
shape where direct output order and exact fallback behavior are easy to prove.

See [BENCHMARK.md](BENCHMARK.md#ch-u14-materialized-runtime-join-filter) for
raw samples and the measured CPU, heap, and allocation tradeoffs.
