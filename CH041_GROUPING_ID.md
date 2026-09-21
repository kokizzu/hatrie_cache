# CH-041: Multi-Argument GROUPING_ID

`GROUPING_ID(expr1, expr2, ...)` is now supported with `GROUPING SETS`,
`ROLLUP`, and `CUBE`.

The first argument is the highest-order bit and the last argument is the
lowest-order bit. An expression contributes `1` when that dimension is absent
from the current grouping set, and `0` when it is present. For example:

```sql
GROUPING_ID(region, product)
```

returns `0` for `(region, product)`, `1` for `(region)`, `2` for `(product)`,
and `3` for the grand-total grouping set.

The implementation validates that every argument is one of the query's
grouping dimensions, accepts one through 63 arguments, and rewrites the value
to a literal while expanding each grouping branch. This keeps the current
grouping execution model and avoids evaluating the arguments for every output
row. `GROUPING_ID` remains `0` for ordinary `GROUP BY` queries, matching the
existing `GROUPING(expr)` fallback.

## Measurement

Target: `make benchmark-ch041-grouping-id`.

Workload: eight output rows from a two-dimension `CUBE`, five samples per
benchmark on Linux/amd64 with an AMD Ryzen 9 5950X.

Raw samples:

```text
BenchmarkCH041GroupingID
67355 42407 408
69263 42408 408
66803 42407 408
69740 42407 408
72105 42409 408

BenchmarkCH041GroupingIDComposedControl
81128 52739 440
74568 52740 440
72670 52740 440
69490 52740 440
69075 52741 440
```

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Composed `GROUPING(a) * 2 + GROUPING(b)` | 72,670 | 52,740 | 440 | 1.00x |
| Native `GROUPING_ID(a, b)` | 69,263 | 42,407 | 408 | 1.05x faster, 1.24x lower heap, 1.08x fewer allocations |

This is a capability and modest efficiency improvement, not the native
one-pass grouping engine described by the larger CH-041 proposal. Grouping
sets still expand into branches, and the 63-argument limit keeps the result
non-negative in the current `int64` SQL value representation.

## Verification

```text
make test-ch041-grouping-id
make race-ch041-grouping-id
make vet-ch041-grouping-id
```
