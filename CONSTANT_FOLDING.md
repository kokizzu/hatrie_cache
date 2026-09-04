# SQL Constant Folding

Hatrie SQL folds deterministic expressions that do not depend on a row while
rewriting the execution-local query tree. This follows the same general
principle as analytical engines that reduce work before the scan or pipeline
starts, while keeping the existing evaluator as the semantic authority.

## Folded Forms

The rewrite pass folds expressions when every input is already a literal:

- `CAST`
- `LOWER`, `COALESCE`, `NULLIF`, and `CONTAINS`
- timestamp parsing and arithmetic functions
- regular-expression and JSON scalar functions
- `CASE`
- `IN` and `BETWEEN`
- `IS NULL`, `IS NOT NULL`, comparisons, boolean operators, and arithmetic

The result is evaluated once during query preparation/rewrite instead of once
per input row. Parameterized queries are folded after parameters are bound.

## Compatibility

Folding is conservative. Row-dependent expressions, aggregate functions,
custom function resolvers, unknown functions, and expressions that return an
evaluation error remain in their original form. The same `evalSQLExpr` code
performs constant evaluation, so SQL null, collation, regular-expression, and
error behavior stays aligned with normal execution.

There is no configuration switch because this is an execution-local rewrite
that does not change the stored or wire format. Queries that do not contain
foldable constants retain their previous plan shape.

## Measurement

Command:

```text
make benchmark-sql-constant-folding
```

The benchmark parses one query twice, executes one copy without the rewrite,
and executes the other after constant folding. It uses 256 input rows and five
one-second repetitions per sub-benchmark.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Without rewrite | 229,340 | 312,153 | 2,066 |
| With constant folding | 110,253 | 225,264 | 1,037 |
| Improvement | **2.08x faster** | **1.39x lower** | **1.99x lower** |

This workload is intentionally constant-heavy. It demonstrates the benefit
of removing repeated expression work, not a universal query-speed promise.
