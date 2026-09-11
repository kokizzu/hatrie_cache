# Automatic Native Ordered SQL

Finite ordered pages over one ordinary `CACHE` or `KEYS` row source now use
the existing native Top-N batch runtime automatically when the shape is
provably safe. The selector supports direct source-field `ORDER BY` keys,
mixed directions, stable ties, and `LIMIT`/`OFFSET` pagination.

The selector remains fail-closed for unbounded ordering, `WITH TIES`,
`DISTINCT`, aggregates, grouping, `HAVING`, joins, windows, `LIMIT BY`, custom
expressions, and any source resolver exposing an optional SQL contract. This
preserves columnar, partition-pruning, index, borrowed-snapshot, streaming,
and ordered-resolver precedence.

Set `SQLQueryOptions.DisableNativeDataflow = true` to force the established
materialized/top-N executor for compatibility testing or an A/B comparison.
The option also disables the automatic scalar, aggregate, and distinct paths.

When observation or slow-query recording is enabled, the plan identifies the
operator as `NATIVE DATAFLOW` with detail
`automatic ordered top-N batch execution`. Ordinary results retain their
existing plan shape.

The paired 4,096-row benchmark is documented in
`BENCHMARK.md#m052r-automatic-native-ordered-top-n`.
