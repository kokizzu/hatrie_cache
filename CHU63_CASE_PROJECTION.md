# CHU63: Columnar CASE Projection

CHU63 adds a conservative columnar fast path for searched `CASE` projections
with exactly one `WHEN` branch. The condition must compare one numeric field
with one numeric literal using `=`, `!=`, `<>`, `<`, `<=`, `>`, or `>=`.
`THEN`, `ELSE`, and missing-`ELSE` results are literal values, including
`NULL`. Direct field projections may appear alongside the CASE item.

The fast path reuses the existing numeric comparison matcher, so `NULL` or
nonnumeric source values take the normal non-match/`ELSE` result. It supports
materialized and streaming query APIs, applies `OFFSET` and `LIMIT` while
scanning, and validates source row counts and result byte budgets. Multiple
branches, dynamic expressions, field-to-field comparisons, predicates,
ordering, grouping, joins, and aggregates retain the general executor.

## Benchmark

Command:

```sh
make benchmark-chu63-case-projection
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has
16,384 packed `int64` values (`row % 4096`) and runs:
`SELECT CASE WHEN value >= 2048 THEN 'high' ELSE 'low' END AS band FROM CACHE('items')`.

| Metric | Before row materialization | After columnar CASE | Improvement |
| --- | ---: | ---: | ---: |
| Median CPU | 7,658,097 ns/op | 3,325,073 ns/op | 2.30x faster |
| Median heap | 14,032,071 B/op | 5,765,200 B/op | 2.43x less |
| Median allocations | 65,570 allocs/op | 48,149 allocs/op | 1.36x fewer |

Raw samples (`ns/op / B/op / allocs/op`):

```text
before: 8061686/14032134/65570, 7753079/14032104/65570, 7658097/14032071/65570, 7945419/14031685/65569, 7813812/14031712/65569
after:  3364901/5765200/48149,   3325073/5765202/48149,   3119660/5765343/48149,   3465421/5765197/48149,   3276927/5765200/48149
```

Unsupported CASE shapes continue to use the general SQL evaluator.
