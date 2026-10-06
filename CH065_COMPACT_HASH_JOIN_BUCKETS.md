# CH-065 Compact SQL Hash-Join Buckets

The SQL hash join previously stored every right-side key as a `[]int`, so a
unique key paid for a slice backing allocation even though it had only one
matching row. The compact bucket stores that first row index directly in the
typed map value. A duplicate key promotes to a posting slice containing all
row indexes in insertion order.

The executor probes the compact reference directly. That keeps the common
unique-key join path free of a temporary one-element slice while retaining the
existing `Lookup` behavior for internal compatibility callers.

Correctness coverage includes numeric values with equivalent integer and float
forms, signed zero, NaN, strings, booleans, duplicate rows, unsupported values,
and multi-way SQL joins. The duplicate path is unchanged semantically: output
order follows right-side insertion order and every matching row is emitted.

The optimization is deliberately local to the hash-join index. It does not
change join selection, NULL behavior, row limits, work accounting, or the
default planner. The tradeoff is one extra indirection through the compact
reference and a small duplicate-row registry; the benchmark shows lower
allocation bytes with unchanged allocation count and no latency regression for
the tested unique-key workloads.

See [BENCHMARK.md](BENCHMARK.md#ch-065-compact-sql-hash-join-buckets) for raw
before/after samples and the measured tradeoff.
