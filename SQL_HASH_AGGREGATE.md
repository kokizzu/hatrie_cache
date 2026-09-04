# SQL Compact Hash Aggregation

Hatrie Cache automatically uses a compact hash aggregation state for a narrow
`GROUP BY` shape. This is inspired by ClickHouse's hash aggregation: the
executor updates one state per group instead of retaining every source row in
every group.

The optimization is admitted when all selected expressions are the single
`GROUP BY` field or one of these direct aggregates:

- `COUNT(*)`
- `COUNT(field)`
- `SUM(field)`
- `AVG(field)`
- `MIN(field)`
- `MAX(field)`

There must be one group field, with no `HAVING`, `DISTINCT`, window expression,
`ORDER BY`, join, union, or `LIMIT BY`. Queries outside this shape use the
existing executor unchanged. A `CACHE` source with `StreamSQLSource` and a
`VALUES` source can update the state while rows stream in; other sources still
use the compact state after normal source materialization.

The path preserves first-seen group order, NULL grouping and aggregate
semantics, query collation, `WHERE`, `OFFSET`/`LIMIT`, callbacks, and existing
execution budgets. When `MaxGroupBytes` is configured, the optimization is
skipped so the established group-memory accounting and spill behavior remain
authoritative. `MaxGroupRowsPerKey` remains enforced while state is updated.

There is no new flag and the default behavior for unsupported queries is
unchanged. The implementation is an internal executor choice and does not
change the SQL or public API.

See [BENCHMARK.md](BENCHMARK.md#sql-compact-hash-aggregation) for raw samples
and the measured comparison.
