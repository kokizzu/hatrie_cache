# CH-G07: GROUPING SETS, ROLLUP, and CUBE

## Status

Implemented and audited in the existing SQL executor. The parser expands
`ROLLUP`, `CUBE`, and `GROUPING SETS` into ordinary grouped branches joined by
`UNION ALL`; each branch reuses the existing grouped aggregate executor.

This gives the expected SQL result semantics without adding a second aggregate
engine. A future optimization could execute all grouping sets in one typed
single-pass aggregate, but that is a separate performance feature and is not
claimed here.

## Semantics

- `ROLLUP(a, b)` produces `(a, b)`, `(a)`, and `()` in that order.
- `CUBE(a, b)` produces all four subsets and is capped at 12 dimensions to
  bound branch expansion.
- `GROUPING SETS ((a, b), (a), ())` preserves the requested set order.
- Dimensions absent from a grouping set are emitted as `NULL`.
- `GROUPING(dimension)` returns `0` when the dimension is present and `1` when
  it is rolled up. Ordinary `GROUP BY` returns `0`.
- Invalid grouping identifiers and use in `WHERE`/`PREWHERE` are rejected.

## Verification

Focused correctness test:

```text
make m270-ch-g07-test
```

The focused race run also passes:

```text
make m272-ch-g07-race
```

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux amd64.

Command:

```text
make m271-ch-g07-benchmark
```

Five runs with `-benchmem -count=5` produced these medians:

| Workload | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| `GROUPING SETS` result expansion | 42,266 | 28,587 | 242 |
| `GROUPING SETS` plus `GROUPING()` identifiers | 51,659 | 33,871 | 286 |

Raw samples, in command order:

```text
grouping_sets:       45123 40903 42663 42266 42198 ns/op; 28587 28588 28587 28587 28587 B/op; 242 242 242 242 242 allocs/op
grouping_identifiers: 51130 51691 51806 51095 51659 ns/op; 33871 33871 33870 33872 33871 B/op; 286 286 286 286 286 allocs/op
```

The raw samples and the broader benchmark index are recorded in
`BENCHMARK.md`.
