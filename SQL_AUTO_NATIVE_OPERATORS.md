# Automatic Native SQL Operators

Plain `CACHE` and `KEYS` queries over an ordinary row resolver automatically
use the existing native batch executor when the compiled shape is provably
supported. This increment covers global `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`
aggregates and one- or two-field `DISTINCT` projections.

The selector is deliberately conservative. It retains the established path
for grouped queries, ordered queries, joins, columnar or borrowed sources,
partitioned sources, index-aware resolvers, streaming resolvers, custom
functions, windows, and any resolver exposing an optional SQL contract. This
keeps source-specific pruning, snapshot, ordering, and storage behavior ahead
of the generic batch optimization.

Set `SQLQueryOptions.DisableNativeDataflow = true` to force the existing
materialized executor for compatibility testing or a controlled comparison.
The option also disables the automatic scalar path documented in
`SQL_AUTO_NATIVE_DATAFLOW.md`.

When query observation or slow-query recording is enabled, the plan reports a
`NATIVE DATAFLOW` operator with the detail
`automatic aggregate/distinct batch execution`. Ordinary results keep their
existing plan shape and do not pay for plan byte accounting.

The paired 4,096-row benchmark is documented in
`BENCHMARK.md#m052q-automatic-native-aggregate-and-distinct`.
