# Two-Level Columnar Aggregation

This is the first implementation from the product-inspiration checklist. It is
inspired by ClickHouse two-level aggregation: independent local aggregate
states are built over bounded input ranges and then merged before projection.
The relevant product context is in the [ClickHouse query optimization
guide](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide).

## Behavior

The path is selected only when all of these conditions hold:

- the query is the existing narrow, single-source columnar `GROUP BY` shape;
- `SQLQueryOptions.Workers` is at least `2`;
- the source has at least `16,384` rows;
- the resolver does not expose custom SQL functions to the columnar matcher;
- there are at least two aggregate projections, all of which are `COUNT`, `MIN`,
  or `MAX`.

`Workers` is capped at the number of 1,024-row blocks and at `16`. The default
`Workers: 0` path and explicit `Workers: 1` path remain single-level. `SUM` and
`AVG` also remain on the established path because combining partial floating
point sums can change addition order. This is a deliberate correctness
boundary, not an unsupported-query error.

Each worker processes a contiguous input range. The parent merges worker states
in range order, which preserves first-seen group order and the existing
aggregate update order for the supported merge-safe states. Result projection,
`OFFSET`, `LIMIT`, `MaxResultBytes`, cancellation, and `MaxGroupRowsPerKey` are
still enforced by the existing contracts.

## Tradeoff

The local maps and aggregate states require more temporary memory. This is why
the feature is explicit through `Workers` and is not enabled by a new default.
Use `Workers: 2` or a small value for a measured, CPU-bound workload; keep the
default for small, cheap, or memory-constrained grouped queries.

The implementation has no storage-format, wire-format, backup, restore, or
replication change. Custom-function resolvers, non-columnar sources, richer SQL
shapes, and configured group-memory budgets retain their existing executor.

## Tests And Commands

The focused red-green test is:

```text
make test-sql-two-level
```

Additional verification targets are:

```text
make test-race-sql-two-level
make vet-sql-two-level
make test-sql-vectorized
make test
```

The repeatable benchmark targets are:

```text
make benchmark-sql-two-level-before-long
make benchmark-sql-two-level-long
```

Both benchmark targets use `-benchmem` and `-benchtime=3s`. The `before` target
uses a temporary worktree with the committed pre-feature aggregation files and
the current surrounding SQL files, so parallel-session changes are not
discarded.

## Raw Results

Machine: Linux, AMD Ryzen 9 5950X, Go benchmark `-benchtime=3s`. Values are
from the two commands above after the supported benchmark was narrowed to
`COUNT(*)`, `MIN(value)`, and `MAX(value)`.

| Workload and path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 32K rows, 257 groups, before, single-level | 7,955,672 | 1,112,618 | 36,684 |
| 32K rows, 257 groups, before, `Workers: 2` control | 7,977,947 | 1,112,638 | 36,684 |
| 32K rows, 257 groups, after, single-level | 8,358,377 | 1,112,626 | 36,684 |
| 32K rows, 257 groups, after, two-level | 6,036,075 | 1,571,464 | 37,013 |
| 32K rows, 32K groups, before, single-level | 138,002,324 | 77,736,813 | 524,566 |
| 32K rows, 32K groups, before, `Workers: 2` control | 128,673,896 | 77,737,372 | 524,565 |
| 32K rows, 32K groups, after, single-level | 132,753,287 | 77,736,755 | 524,566 |
| 32K rows, 32K groups, after, two-level | 136,583,958 | 91,565,670 | 524,946 |

The repeated-group workload improves from `7,977,947` to `6,036,075 ns/op`, or
`1.32x` faster, with `1.41x` higher allocation volume and `1.01x` allocations.
The unique-group workload is `1.03x` slower than its same-run single-level
control and uses `1.18x` the allocation volume, so it does not justify forcing
the path automatically. This is the reason the feature remains caller-selected
and the high-cardinality behavior remains documented as workload-dependent.
