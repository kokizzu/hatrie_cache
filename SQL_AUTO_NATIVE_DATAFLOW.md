# Automatic Native SQL Dataflow

Plain materialized SQL projections over ordinary row resolvers use the native
batch dataflow runtime automatically. The selector is intentionally narrow:

- one `CACHE` or `KEYS` source;
- scalar `SELECT` expressions and an optional scalar `WHERE`;
- no joins, grouping, ordering, `DISTINCT`, windows, sampling, CTEs, or set
  operations;
- no columnar, streaming, lookup, index, or ordered resolver contract;
- no optimizer, index hint, worker, advisor, recorder, or custom collation
  option.

The default path preserves the existing SQL result shape and source/result
budgets. `QueryResult.Plan` and privacy-safe query observations identify the
selected operator as `NATIVE DATAFLOW`.

Set `SQLQueryOptions.DisableNativeDataflow = true` to use the existing
materialized executor for a compatibility fallback or an A/B comparison. The
flag does not affect `ExecuteSQLQueryRows`, which retains its streaming
contract.

The selector avoids specialized resolver implementations because those
contracts can use columnar late materialization, direct indexes, ordered
iteration, or bounded streaming. Unsupported runtime expressions and richer
query shapes therefore remain on the established executor.
