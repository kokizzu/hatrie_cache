# Canonical Compiled-Plan Equivalence

`hatSql.SQLCompiledQueryCache` first checks the exact source string, then uses
the existing unambiguous token-normalized prepared-cache key on a miss. SQL
that differs only by whitespace or keyword casing shares one immutable
`CompiledSQLQuery`. Literal values, identifiers, and schema-version namespaces
remain distinct, so this does not merge queries with different results.

The cache stays bounded by its existing entry and byte limits. Canonical
entries share one LRU position, and eviction/invalidation removes both the
raw and canonical lookup paths. Exact-source hits do not lex or allocate.

This is a scoped MZ-045 adoption. It improves compiled-plan reuse, but it does
not yet connect literal-independent query fingerprints to arrangement choices.

See the benchmark details in [BENCHMARK.md](BENCHMARK.md#mz-045-canonical-compiled-plan-equivalence).
