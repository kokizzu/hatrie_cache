# SQL Improvement Backlog: 100 Measured Candidates

## Goal

Evaluate these candidates in priority order with a focused regression test,
baseline benchmark, smallest correct change, repeat benchmark, and broad
verification. Keep only changes that improve the named workload without
breaking result, NULL, collation, ordering, cancellation, spill-budget, or
cleanup semantics.

Items are candidates, not promises. `P0` means likely high return and ready for
measurement; `P1` needs a workload gate; `P2` is useful only after profiling.

## Columnar And Expression Execution

1. `P0` Vectorize numeric `BETWEEN` filters in `executeSQLColumnarScan`.
2. `P0` Vectorize numeric `IN` predicates with a compact typed membership set.
3. `P0` Vectorize `IS NULL` and `IS NOT NULL` over column presence bitmaps.
4. `P0` Fuse conjunctions of direct numeric predicates into one scan.
5. `P0` Fuse direct numeric predicates with projection and `LIMIT` stopping.
6. `P0` Add direct columnar `COUNT(field)` null-aware aggregation.
7. `P0` Add direct columnar `SUM`/`AVG` grouped by one dictionary field.
8. `P0` Add direct columnar `MIN`/`MAX` grouped by one dictionary field.
9. `P0` Add direct dictionary `IN` filtering without materialized rows.
10. `P0` Add direct dictionary prefix filtering when binary collation permits.
11. `P1` Evaluate simple arithmetic projection from numeric columns.
12. `P1` Evaluate `CASE` on a direct numeric predicate in the column scan.
13. `P1` Add a columnar boolean representation instead of interface values.
14. `P1` Add typed int64, float64, timestamp, and date column vectors.
15. `P1` Store nullable vectors as values plus a null bitmap.
16. `P1` Add a selection-vector representation for generic vector predicates.
17. `P1` Select a predicate order by vector statistics and selectivity.
18. `P1` Push projection field collection before JSON batch decoding.
19. `P1` Permit narrow direct scans for `OFFSET` plus `LIMIT` when ordered.
20. `P2` Add SIMD only after scalar typed-vector baselines demonstrate a gap.

## Source Decode, Representation, And Memory

21. `P0` Measure source-byte copying during `ResolveSQLColumnarSource`.
22. `P0` Add an internal read-snapshot borrowed-byte contract if ownership is safe.
23. `P0` Decode JSON only for fields referenced by the resolved query plan.
24. `P0` Cache parsed column schema per source generation.
25. `P0` Reuse JSON decoder scratch buffers within one query.
26. `P1` Prefer a compact binary columnar source format for new SQL collections.
27. `P1` Make source encoding selectable with JSON compatibility fallback.
28. `P1` Store repeated string dictionaries in the persisted columnar format.
29. `P1` Delta-code ordered int64/date/timestamp vectors where profitable.
30. `P1` Bit-pack low-cardinality integers and booleans.
31. `P1` Add a per-query arena for transient `sqlExecRow` and key buffers.
32. `P1` Reuse projection row maps only on streaming APIs with clear ownership.
33. `P1` Pre-size generic row maps from source schema width.
34. `P1` Intern repeated field names and aliases in parsed plans.
35. `P1` Cache canonical field lookup maps by source schema generation.
36. `P2` Pool large scan buffers behind size caps and clear-reference tests.
37. `P2` Add allocation telemetry per execution-plan node.
38. `P2` Measure GC pause and retained heap for long columnar scans.
39. `P2` Add an adaptive row/column threshold only with a stable workload signal.
40. `P2` Compact sparse columns with offset tables after density profiling.

## Indexes, Predicate Pushdown, And Partitions

41. `P0` Use typed numeric/date keys for range-index probes instead of text keys.
42. `P0` Push conjunctions into composite-index prefix/range probes.
43. `P0` Add cost comparison between index probe and direct columnar scan.
44. `P0` Exploit covering indexes before fetching full source rows.
45. `P0` Push `LIMIT` into ordered index scans.
46. `P0` Reuse equality probe results across repeated join keys.
47. `P1` Add selectivity statistics per secondary-index key.
48. `P1` Add lightweight histograms for numeric range predicates.
49. `P1` Add top-value statistics for skewed dictionary columns.
50. `P1` Intersect bitmap indexes for multi-predicate filters.
51. `P1` Union bitmap indexes for eligible `OR` predicates.
52. `P1` Build dictionary-id bitmap indexes for low-cardinality strings.
53. `P1` Add index-only `COUNT(*)` and `COUNT(field)` paths.
54. `P1` Stream grouped aggregation from composite-index order.
55. `P1` Stream `DISTINCT` from matching index order with `LIMIT` pushdown.
56. `P1` Prune partitions using `IN`, `BETWEEN`, and conjunctions.
57. `P1` Prune timestamp partitions after safe cast/literal normalization.
58. `P1` Cache partition-boundary metadata by generation.
59. `P2` Make index-maintenance batching aware of transaction commit groups.
60. `P2` Add index-build throttling and progress telemetry for large collections.

## Joins And Subqueries

61. `P0` Benchmark hash join build/probe side selection across skewed inputs.
62. `P0` Base join choice on exact source count when inexpensive to obtain.
63. `P0` Add a compact typed hash-key encoding for numeric and boolean joins.
64. `P0` Reuse a build-side hash table for identical repeated subquery joins.
65. `P0` Push single-alias `WHERE` terms below eligible inner joins.
66. `P1` Push compatible predicates into outer joins without changing NULL semantics.
67. `P1` Use index nested-loop joins for small outer inputs after cost comparison.
68. `P1` Batch index probes for repeated outer join keys.
69. `P1` Add a merge join when both sources are provably ordered on the join key.
70. `P1` Add bloom prefiltering for in-memory hash joins with skew gates.
71. `P1` Use partition-size histograms to tune spill hash join partition count.
72. `P1` Dynamically split only oversized spill hash partitions.
73. `P1` Compact join match tracking for left/right/full joins with bitsets.
74. `P1` Avoid constructing empty alias maps repeatedly for unmatched outer rows.
75. `P1` De-correlate eligible scalar correlated subqueries into joins.
76. `P1` Cache uncorrelated scalar subquery results within an execution.
77. `P2` Add batched lateral subquery evaluation for shared arguments.
78. `P2` Add join-order dynamic programming only for a small, capped join count.
79. `P2` Record actual versus estimated rows in `EXPLAIN ANALYZE` feedback.
80. `P2` Use prior execution cardinality feedback only with generation invalidation.

## Sort, Group, Distinct, Windows, And Spill

81. `P0` Replace `fmt.Sprintf` spill-size estimation with typed size accounting.
82. `P0` Reuse sort-key buffers while building spill records.
83. `P0` Use a bounded heap for `ORDER BY ... LIMIT` before creating full runs.
84. `P0` Use Top-N selection for `ORDER BY ... OFFSET ... LIMIT` where bounded.
85. `P0` Parallelize external set merge passes with the existing budget discipline.
86. `P0` Add parallel spill hash partition encoding when source streaming allows it.
87. `P1` Evaluate a typed spill codec against `gob` for CPU, bytes, and recovery.
88. `P1` Select compression from observed spill entropy, not a fixed flag alone.
89. `P1` Reuse compression buffers for successive spill files.
90. `P1` Reduce set-operation double sorting when output order is not requested.
91. `P1` Use incremental aggregation for sorted/grouped input before spill.
92. `P1` Add dictionary-key group state instead of formatted composite string keys.
93. `P1` Add partitioned in-memory aggregation for high-cardinality groups.
94. `P1` Spill partial aggregate states using typed aggregate encodings.
95. `P1` Use index order for window partition/order input more broadly.
96. `P1` Stream bounded window frames with a ring buffer.
97. `P1` Share sort order between compatible windows and final `ORDER BY`.
98. `P2` Add radix sort for fixed-width numeric spill/order keys after benchmarks.
99. `P2` Tune merge fan-in from file descriptor and storage throughput budgets.
100. `P2` Add per-operator CPU, allocations, peak memory, spill bytes, and rows to `EXPLAIN ANALYZE`.

## First Measured Batch

Start with items `2`, `4`, `7`, `9`, `21`, `23`, `41`, `43`, `45`, `61`,
`63`, `65`, `81`, `83`, `85`, and `100`. They extend paths that already
exist in `hat/hatSql/query.go` and `hat/hatCache/sql_query.go`, have focused
test seams, and do not require a public SQL syntax change.

For each accepted item, record before/after `ns/op`, `B/op`, `allocs/op`,
peak memory where relevant, output correctness coverage, and the workload
shape. Reject it when the named workload regresses or its resource cost is not
justified.
