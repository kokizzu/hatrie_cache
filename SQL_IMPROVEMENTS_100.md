# SQL Improvements: 100 Candidate Backlog

This source-based backlog was produced from `make audit-sql-improvements-100` on 2026-08-29. It is intentionally a candidate list, not a claim that every item is missing or appropriate for every deployment. Items are grouped so a future goal can select a coherent, testable slice rather than mixing unrelated changes.

The current temporal and analytics goal remains active. The goal system cannot replace an unfinished goal, so the prioritized selection below is prepared as the next goal once the active work is complete.

## Query Planning And Costing

1. **P0, medium:** Collect table row-count statistics so plans do not assume all sources cost the same.
2. **P0, medium:** Collect per-column distinct-count statistics for join and grouping cardinality estimates.
3. **P1, medium:** Record null fractions and value ranges for selectivity estimation.
4. **P1, high:** Add histogram statistics for skewed numeric, date, and timestamp predicates.
5. **P0, medium:** Choose join order by estimated cost instead of input order.
6. **P0, medium:** Choose hash, nested-loop, or merge join from estimated rows and available ordering.
7. **P1, medium:** Push predicates through derived tables and common table expressions when semantics permit.
8. **P1, medium:** Push projection pruning into all source readers, not only direct cache scans.
9. **P1, medium:** Rewrite decorrelatable subqueries into joins or semi-joins.
10. **P1, low:** Include estimated versus actual rows and cost in EXPLAIN ANALYZE output.

## Execution And Memory

11. **P0, medium:** Use vectorized expression evaluation for batches of rows.
12. **P0, medium:** Reuse typed value buffers across executor operators to reduce per-row allocation.
13. **P1, medium:** Add a memory-accounted hash aggregation that spills partitions before admission limits are exceeded.
14. **P1, medium:** Add external merge sort for unbounded ORDER BY workloads.
15. **P1, medium:** Partition oversized hash joins and spill deterministically.
16. **P1, medium:** Support adaptive batch sizes based on row width and remaining query memory.
17. **P2, high:** Parallelize independent scan, filter, and aggregate partitions with bounded worker pools.
18. **P1, medium:** Add runtime filter propagation from hash joins to upstream scans.
19. **P1, low:** Fuse filter and projection operators when no observable diagnostics are lost.
20. **P2, medium:** Add cooperative yielding for long CPU-bound operators so cancellation latency stays bounded.

## Indexes And Access Paths

21. **P0, high:** Add typed secondary indexes for integer, date, timestamp, and boolean equality/range predicates.
22. **P0, high:** Add composite indexes with left-prefix matching for common multi-column filters and orderings.
23. **P1, medium:** Add covering-index scans that avoid fetching base values for projected columns.
24. **P1, medium:** Add partial indexes guarded by a stable predicate.
25. **P1, medium:** Add expression indexes for normalized text, extracted JSON paths, and computed dates.
26. **P1, medium:** Select an index automatically using statistics; retain hints only as an explicit override.
27. **P1, medium:** Add index intersection for independent selective equality predicates.
28. **P2, medium:** Add index union for OR predicates with duplicate elimination.
29. **P1, high:** Add range-friendly time indexes for event-time scans and retention deletion.
30. **P2, high:** Add full-text inverted indexes with explicit tokenizer and collation configuration.

## Relational Coverage

31. **P1, medium:** Support RIGHT JOIN by normalization to a tested equivalent plan.
32. **P1, high:** Support FULL OUTER JOIN with correct null-extension and duplicate handling.
33. **P1, medium:** Support USING and NATURAL join column coalescing with unambiguous output names.
34. **P1, medium:** Add anti-join planning for NOT EXISTS and NOT IN with SQL null semantics.
35. **P1, medium:** Add quantified comparisons: ANY, SOME, and ALL.
36. **P2, medium:** Support recursive common table expressions with depth, row, and cycle limits.
37. **P1, medium:** Support MERGE with deterministic matching and duplicate-match errors.
38. **P1, medium:** Add RETURNING for mutations so callers avoid a follow-up read.
39. **P2, medium:** Add updatable views with explicit eligibility rules.
40. **P2, medium:** Support row-value constructors and multi-column comparisons.

## Analytics And Windows

41. **P1, medium:** Implement missing standard window functions with shared frame evaluation.
42. **P1, medium:** Support RANGE and GROUPS window frames with typed peer comparison.
43. **P1, medium:** Support frame exclusion: CURRENT ROW, GROUP, and TIES.
44. **P1, medium:** Add percentile_cont and percentile_disc exact aggregates.
45. **P1, medium:** Add ordered-set aggregate syntax for percentile and mode functions.
46. **P1, medium:** Add FILTER support consistently to all built-in aggregates and UDF aggregates.
47. **P1, medium:** Optimize grouping sets, ROLLUP, and CUBE with shared partial aggregates.
48. **P2, medium:** Add GROUPING and GROUPING_ID output for grouping-set disambiguation.
49. **P1, medium:** Add top-k aggregate with deterministic tie handling.
50. **P2, medium:** Add analytic covariance, correlation, regression, and standard-deviation functions.

## Temporal And Streaming SQL

51. **P0, medium:** Integrate temporal AS OF queries with parser, planner, and table sources rather than only the helper API.
52. **P0, medium:** Integrate watermarks and late-event policy into streaming query execution.
53. **P1, medium:** Add tumbling, hopping, and session SQL window syntax over event time.
54. **P1, high:** Add stream-stream joins with bounded state and watermark-based eviction.
55. **P1, medium:** Add temporal lookup joins against versioned dimension tables.
56. **P1, medium:** Make event-time versus processing-time explicit in functions and diagnostics.
57. **P1, medium:** Add late-event side outputs and replay-safe correction modes.
58. **P2, high:** Add exactly-once checkpoint coordination for subscriptions and materialized streaming views.
59. **P1, medium:** Add schedule-aware rollup refresh with idempotent watermark progress.
60. **P1, medium:** Add queryable stream-state metrics: lag, state bytes, late rows, and watermark age.

## Types, Expressions, And Semantics

61. **P1, low:** Add interval and period types with calendar-aware arithmetic.
62. **P1, medium:** Add fixed-width decimal precision and scale enforcement at parse, bind, and arithmetic boundaries.
63. **P1, medium:** Add UUID, IP address, and network containment functions with index-aware operators.
64. **P1, medium:** Add array type, array functions, and ANY/ALL membership semantics.
65. **P1, medium:** Add structured JSON constructors, mutation functions, and JSON_TABLE-style projection.
66. **P1, medium:** Add generated columns with deterministic-expression validation.
67. **P1, low:** Complete SQL three-valued logic tests across joins, subqueries, and set operations.
68. **P2, medium:** Add locale-aware collations with version-pinned behavior for persisted indexes.
69. **P1, low:** Add standards-compatible literal escaping and binary/hex literal forms.
70. **P2, medium:** Add user-defined scalar type aliases with explicit cast rules.

## Durability, Recovery, And Storage

71. **P0, medium:** Define transactional atomicity for multi-row SQL mutations and make rollback behavior testable.
72. **P0, medium:** Add write-ahead intent records for durable mutation recovery.
73. **P1, medium:** Add crash-consistent secondary-index rebuild and validation tooling.
74. **P1, medium:** Add online schema-change state transitions with resumable backfill.
75. **P1, medium:** Add consistent read snapshots across all tables participating in a query.
76. **P1, medium:** Add compaction-aware query pinning so readers retain the exact required files or generations.
77. **P1, low:** Add backup manifests containing schema, version, checksum, and dependency order.
78. **P1, medium:** Add point-in-time restore from snapshots plus mutation log segments.
79. **P1, medium:** Add storage checksums and corruption quarantine before query execution reads damaged data.
80. **P2, medium:** Add tiered hot/warm/cold storage policies with explicit query-latency reporting.

## Security And Governance

81. **P0, medium:** Enforce table, column, and function privileges at bind and execution time.
82. **P0, medium:** Add row-level security predicates with policy recursion protection.
83. **P1, medium:** Add masked or tokenized column projections for sensitive fields.
84. **P1, low:** Add query result-size and export-rate limits per principal.
85. **P1, medium:** Make audit events tamper-evident with a chained hash and durable sink acknowledgement.
86. **P1, medium:** Add resource quotas by tenant, role, and workload class.
87. **P1, medium:** Add explicit allowlists for external file paths, HTTP hosts, and plugin capabilities.
88. **P1, low:** Redact literals and secret-bearing parameters in diagnostics, telemetry, and EXPLAIN output.
89. **P2, medium:** Add signed policy bundles and rollout/rollback versioning.
90. **P1, low:** Add security regression tests for privilege escalation through views, functions, and virtual sources.

## Interfaces, Operations, And Developer Tooling

91. **P0, medium:** Expose a stable wire protocol with typed parameters, streaming rows, cancellation, and structured errors.
92. **P1, medium:** Add prepared-statement plan invalidation when schemas, indexes, functions, or policies change.
93. **P1, low:** Add query-plan snapshots and golden tests for optimizer regression detection.
94. **P1, medium:** Add deterministic fuzzing for parser, binder, planner, and executor equivalence properties.
95. **P1, medium:** Add SQL logic-test compatibility runner for portable semantic coverage.
96. **P1, medium:** Add differential tests against a reference engine for the supported SQL subset.
97. **P1, low:** Add workload capture and replay with parameter redaction and stable fixtures.
98. **P1, medium:** Add benchmark suites for each supported command, data shape, concurrency, allocation, and memory high-water mark.
99. **P1, low:** Publish a machine-readable supported-SQL matrix linked to parser and execution tests.
100. **P1, medium:** Add an operator handbook for diagnosing spills, plans, quotas, snapshots, replicas, backups, and recovery.

## Recommended Next Goal

Start with items 1, 2, 5, 6, 10, 12, 21, 22, 26, and 93. Together they form a bounded optimizer and typed-index goal: collect cheap statistics, plan joins and indexes from those statistics, remove avoidable executor allocations, and lock the behavior with plan snapshots. It is a high-leverage performance increment without introducing a new storage or distributed-system contract.

Defer parallel execution, recursive CTEs, full-text search, exactly-once streaming, point-in-time restore, and fine-grained authorization to separate goals. Each changes a broad correctness or operational contract and should not be mixed with optimizer work.
