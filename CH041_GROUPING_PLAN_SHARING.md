# CH-041 Grouping Branch Plan Sharing

`GROUPING SETS`, `ROLLUP`, and `CUBE` are represented as `UNION ALL` branches
by the SQL parser. Each branch changes only grouping expressions and the
expressions that expose grouping dimensions.

The branch builder now shares the immutable source, join, filter, CTE, and
execution-plan descriptors. It deep-copies only branch-local rewrite state:
`SELECT`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT BY` expressions. The
existing full query clone remains available for unrelated query cloning.

This lowers parse/plan construction cost without changing the execution model:
the current implementation still executes one aggregate branch per grouping
set. Native one-pass grouping, shared aggregate state, and multi-argument
`GROUPING_ID` remain separate follow-up work.

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
