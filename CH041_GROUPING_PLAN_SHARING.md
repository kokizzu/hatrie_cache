# CH-041 Grouping Branch Plan Sharing

`GROUPING SETS`, `ROLLUP`, and `CUBE` are represented as `UNION ALL` branches
by the SQL parser. Each branch changes only grouping expressions and the
expressions that expose grouping dimensions.

The branch builder now shares the immutable source, join, filter, CTE, and
execution-plan descriptors. It deep-copies only branch-local rewrite state:
`SELECT`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT BY` expressions. The
existing full query clone remains available for unrelated query cloning.

This lowers parse/plan construction cost. Simple aggregate projections now also
use a native one-pass grouping executor that scans the filtered input once and
keeps compact aggregate state for every grouping set. Queries with richer
semantics retain the branch-per-set executor. Multi-argument `GROUPING_ID`
remains separate follow-up work.

## Measurement

On the same AMD Ryzen 9 5950X host, ten samples were collected with Go
benchmarks. The pre-change comparison used commit `4855231c`.

| Workload | Before | After | Result |
| --- | ---: | ---: | ---: |
| One grouping branch construction | 23,162 ns/op, 30,128 B/op, 274 allocs/op | 2,566 ns/op, 4,512 B/op, 6 allocs/op | 9.03x faster, 6.68x less temporary memory, 45.67x fewer allocations |
| 3-set `GROUPING SETS` query | 40,402 ns/op, 28,430 B/op, 242 allocs/op | 38,329 ns/op, 28,429 B/op, 242 allocs/op | 1.05x faster, memory unchanged |
| 16-set `CUBE` query | 900,465 ns/op, 395,735 B/op, 6,757 allocs/op | 854,742 ns/op, 395,720 B/op, 6,757 allocs/op | 1.05x faster, memory and allocations effectively unchanged |

The end-to-end result is modest because aggregate result materialization still
dominates this small workload. The change is retained because the plan-builder
win is large and there was no correctness, memory-retention, or allocation
regression in the complete query benchmark.

## Native one-pass execution

The same four-row, four-dimension `CUBE` workload was measured with five
`-benchmem` samples on the same host. The expanded `UNION ALL` path was forced
as the control; the one-pass path is automatic for eligible queries.

| Executor | Median ns/op | Median B/op | Median allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Expanded `UNION ALL` branches | 610,765 | 310,191 | 6,577 | baseline |
| Native one-pass grouping | 206,758 | 184,539 | 2,173 | 2.95x faster, 40.5% less allocated memory, 3.03x fewer allocations |

Eligibility is deliberately conservative: simple `COUNT`, `SUM`, `AVG`,
`MIN`, and `MAX` projections, grouping dimensions, `GROUPING(expr)`, and
ordinary filtering are supported. Joins, CTEs, `HAVING`, `ORDER BY`, `LIMIT`,
windows, `DISTINCT`, samples, custom functions, configured group-memory
tracking, and richer projections fall back to the existing executor.

The raw samples and benchmark command are recorded in
[BENCHMARK.md](BENCHMARK.md#ch-041-native-one-pass-grouping-sets).
