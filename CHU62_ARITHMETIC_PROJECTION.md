# CHU62: Columnar Arithmetic Projection

CHU62 adds a conservative columnar fast path for direct numeric arithmetic in
`SELECT` projections. It handles a numeric column combined with one numeric
literal using `+`, `-`, `*`, `/`, or `%`, in either operand order. It supports
both materialized query results and `ExecuteSQLQueryRows` streaming.

The executor reuses the existing `sqlArithmeticValue` evaluator after reading
the column value. This preserves integer results, floating-point results,
`NULL` propagation, and division/modulo-by-zero behavior. `OFFSET` and `LIMIT`
are applied while scanning, and only referenced columnar fields are resolved.

Field-to-field arithmetic, nested expressions, dynamic values, aggregates,
ordering, grouping, joins, and predicates retain the general executor. The
fast path is therefore a plan admission optimization rather than a semantic
change.

## Benchmark

Command:

```sh
make benchmark-chu62-arithmetic-projection
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has
16,384 packed `int64` values and runs:
`SELECT value + 1 AS incremented FROM CACHE('items')`.

| Metric | Before row materialization | After columnar projection | Improvement |
| --- | ---: | ---: | ---: |
| Median CPU | 7,872,228 ns/op | 3,333,242 ns/op | 2.36x faster |
| Median heap | 14,159,275 B/op | 5,898,958 B/op | 2.40x less |
| Median allocations | 81,694 allocs/op | 65,044 allocs/op | 1.26x fewer |

Raw samples (`ns/op / B/op / allocs/op`):

```text
before: 7872228/14160073/81694, 8082407/14159260/81693, 8190199/14159285/81694, 7558430/14159275/81693, 7428357/14159271/81693
after:  3403258/5899126/65044,   3419159/5899142/65044,   3101181/5898956/65043,   3189964/5898958/65043,   3333242/5898956/65043
```

The row path remains available for unsupported query shapes and for sources
that do not implement `SQLColumnarSourceResolver`.
