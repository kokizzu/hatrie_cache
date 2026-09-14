# Read-In-Order LIMIT

Eligible direct `CACHE` queries with one ordered index already have a streaming
executor. The materialized `ExecuteSQLQuery` path now propagates its stop signal
to that executor, so it stops after `OFFSET + LIMIT` qualifying rows instead of
materializing the entire ordered source. `LIMIT 0` visits no source rows.

The complete `WHERE` expression, projection, result-byte budget, and source row
budget remain unchanged. Queries with unsupported order shapes, `WITH TIES`,
`LIMIT BY`, joins, grouping, or expressions that cannot use the ordered index
retain their existing path. No storage or wire format changes are required.

When callers set an explicit positive `MaxRows` source budget, early stopping is
disabled so an oversized ordered source still returns the established budget
error.

See the raw before/after measurement in
[BENCHMARK.md](BENCHMARK.md#ch-021-read-in-order-early-limit-completion).
