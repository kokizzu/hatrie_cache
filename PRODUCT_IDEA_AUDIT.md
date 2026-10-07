# Product idea audit — 2026-10-07

This audit separates catalog wording from implementation evidence. The source
baseline is `f4039d3d`, plus the verified aggregate recovery fix `8c766c9c`.
The original 150 identifiers are retained, alongside four additional Tarantool
ideas. Structural completeness does not prove all adoption or performance claims.

## Reconciled stale entries

| Idea | Implementation and test evidence | Supported boundary and remaining work |
| --- | --- | --- |
| CH-U40 | [Result cache](hat/hatSql/result_cache.go), [SQL integration](hat/hatSql/sql_result_cache.go), [regressions](hat/hatSql/chu40_dependency_invalidation_test.go) | Selective source invalidation, explicit/versioned SQL paths, persistence, eviction, and in-flight invalidation are implemented. Explicit mode requires mutation notifications; per-partition dependencies are not provided by this API. |
| CH-U45 | [SQL tie-boundary tests](hat/hatSql/limit_with_ties_test.go), [contract](LIMIT_WITH_TIES.md) | Finite ordered limits support tied rows, offset, zero limit, forced external spill, and row-streaming fallback. The previous claim that SQL lacks WITH TIES was false. Large tie groups still require proportionate output work. |
| CH-U46 | [ASOF executor](hat/hatSql/asof_join.go), [SQL tests](hat/hatSql/asof_join_test.go), [contract](SQL_ASOF_JOIN.md) | Inner/left ASOF joins exist with exactly one equality and one temporal inequality. Tests assert nearest prior matches, strict time, unmatched left rows, and unsupported-condition rejection. This is not arbitrary temporal join planning. |
| CH-U47 | [Dictionary implementation](hat/hatSql/external_dictionary.go), [SQL/lifecycle tests](hat/hatSql/external_dictionary_test.go), [contract](SQL_EXTERNAL_DICTIONARIES.md) | SQL lookup/default/existence functions, immutable refresh, failed-refresh retention, staleness, and close exist. Loader authentication and external I/O remain caller-owned. |
| M-U45 | [Selector](hat/hatSql/typed_table_join_arrangement_selection.go), [tests](hat/hatSql/m_u45_join_arrangement_selection_test.go), [contract](MU045_JOIN_ARRANGEMENT_SELECTION.md) | Explicit alternatives are ranked deterministically; tests cover cost, sharing, explanation, and ambiguous inputs. Predicate equivalence, live acquisition/hydration/release, and automatic planner integration remain caller responsibilities. |

These code paths were included in the successful full `hat/hatSql` run at
`8c766c9c` preparation (`make codex-mu031-recovery-package`, 6.545 seconds).
This documentation change leaves those sources unchanged. Their test assertions
were inspected for the boundaries above. The run did not enable optional build
tags, exercise real remote services, or establish new performance results.
Historical measurements remain in each feature document and `BENCHMARK.md`;
they were not rerun for this catalog correction.

## Additional verified progress

M-U31 already had transactional aggregate callbacks in `c5f3bd97`, rather than
being a missing implementation. Review found that failed restoration left a
transaction committable. Commit `8c766c9c` closes that failed-recovery boundary,
retains both error causes, and adds Add/Retract/Merge error/panic regressions.
Focused, package, race, and vet checks passed. Sequential measurements show
unchanged allocation counts and essentially unchanged successful Add latency.
See [the regression report](MU031_RETRACTABLE_AGGREGATES.md#failed-restoration-regression-2026-10-07).

## Catalog integrity and next audit scope

`make verify-product-idea-catalog` passes with 150 original and four additional
ideas. The check is reused from `d3e3cb5d`; this reconciliation preserves the
newer baseline's CH-U03 registry, T-U44 memory diagnostics, and T-U53–55 entries
instead of overwriting them with an older branch's catalog. T-U01 was recovered
from original catalog commit `2705e5fe` and checked against the current
`hatPeer.ConnectionPool` contract.

The remaining rows still require source/test/benchmark audits before a global
completion claim. In particular, partial library primitives must not be counted
as automatic SQL/storage integration. T-U23's typed multikey structure and
existing SQL multikey paths need a combined audit before adding nested-array or
space-lifecycle work; published history already contains several implementations.
Likewise, CH-U05 partitioned window spill, CH-U22 SQL append routing, CH-U26
automatic operator memory recording, and M-U27 durable publication history have
follow-up branches that must be checked before implementing them again.

The shared checkout remains untouched by these feature commits. Its mixed
staged/unstaged changes are not an integration baseline. No samples were copied;
the earlier samples-symlink instruction remains applicable, but its exact source
and destination paths have not been recovered.
