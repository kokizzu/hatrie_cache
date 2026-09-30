# C193 LIMIT 0 Source Short-Circuit

Direct `CACHE` and `KEYS` queries with `LIMIT 0` and explicit projections now
return the projected column names without reading the source. This follows the
ClickHouse-style optimization for schema probes and impossible-result queries.

The shortcut is deliberately narrow. It does not apply to `SELECT *`, joins,
unions, CTEs, subqueries, final sources, totals, sampled queries, or
`LIMIT ... WITH TIES`; those paths retain their existing metadata and
validation behavior. Nonzero limits are unchanged.

The result keeps a non-nil empty `Rows` slice for compatibility with existing
query consumers. Row streaming uses the same short circuit and invokes no row
callback.

## Verification

Focused tests cover materialized and streamed queries, explicit column names,
source-read avoidance, nonzero-limit behavior, and the `SELECT *` exclusion.
See [BENCHMARK.md](BENCHMARK.md#c193-limit-zero-source-short-circuit) for
before/after CPU, memory, and allocation measurements.
