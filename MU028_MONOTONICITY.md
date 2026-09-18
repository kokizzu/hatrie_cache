# M-U28 SQL Monotonicity Inference

`hatSql.AnalyzeSQLExpressionMonotonicity` is an opt-in, conservative proof
helper for callers that need to decide whether an expression can preserve an
ordered input path. It analyzes one SQL expression relative to one input
column and never changes the SQL planner or execution defaults.

```go
report, err := hatSql.AnalyzeSQLExpressionMonotonicity("price >= 10", "price")
if err != nil {
	return err
}
fmt.Println(report.Monotonicity)
// non-decreasing
```

The report includes:

- `Monotonicity`: `constant`, `non-decreasing`, `non-increasing`, or
  `unknown`.
- `DependsOnColumn`: whether the proof depends on the selected input column.
- `MayReturnNull`: whether SQL NULL can be produced.
- `Reason`: a short explanation of the proof or conservative fallback.

## Proven Rules

The current proof set is intentionally small:

| Expression shape | Result |
|---|---|
| Literal, including `NULL` | Constant |
| Selected field | Non-decreasing |
| Unary `-` or `NOT` | Flips a proven direction |
| `+` or `-` with one constant side | Preserves or flips direction |
| `*` with a numeric constant | Zero becomes constant; positive preserves; negative flips |
| `/` by a non-zero numeric constant | Positive preserves; negative flips; zero is unknown |
| Ordered comparison with one constant side | Threshold direction follows the ordered input |
| `AND`/`OR` with a boolean constant | Constant or neutral side preserves direction |

Unsupported functions, joins, aggregates, subqueries, `CASE` branch proofs,
membership predicates, equality predicates, unrelated fields, and incompatible
expression combinations return `unknown`. A zero scale removes dependency on
the selected column, but the result can still be NULL when the input is NULL.

## Correctness Boundary

The analyzer has no schema, collation, type-range, or NULL-order metadata. The
direction describes the non-NULL value domain and `MayReturnNull` is surfaced
separately. Callers must supply the actual type and NULL ordering before using
the result to skip sorting, maintain an arrangement, or admit an incremental
plan. In particular, callers must account for numeric overflow/error policy,
floating-point edge values, collation, casts, retractions, and changing
function/session behavior.

An expression with an unqualified column name matches the same field name even
when the parsed expression has a qualifier. A qualified target such as
`orders.price` requires the same qualifier. The existing SQL lexer/parser is
used, so malformed expressions and trailing tokens return parser diagnostics.

This is a public analysis primitive, not a claim that existing queries became
faster. A future planner integration should add schema-aware proof tests and
benchmark the complete query path before enabling any new fast path.

## Measurement

`make benchmark-mu028-monotonicity-baseline` parses the same expression tree
without walking it. `make benchmark-mu028-monotonicity` adds the monotonicity
walk. Each uses five one-second samples on Linux/amd64 with an AMD Ryzen 9
5950X. The feature is a new opt-in call, so this is overhead accounting, not
an execution-speed improvement claim.

| Expression | Parser median | Analyzer median | Extra CPU | Parser memory | Analyzer memory |
|---|---:|---:|---:|---:|---:|
| `price + 10` | 1,227 ns/op | 1,324 ns/op | 7.9% | 1,400 B/op, 6 allocs | 1,400 B/op, 6 allocs |
| `price >= 10` | 1,172 ns/op | 1,251 ns/op | 6.7% | 1,080 B/op, 5 allocs | 1,080 B/op, 5 allocs |
| `abs(price)` | 1,183 ns/op | 1,359 ns/op | 14.9% | 1,088 B/op, 6 allocs | 1,088 B/op, 6 allocs |

The analyzer adds no measured allocation or retained bytes for these cases.
The caller should cache reports for prepared expressions when the same proof is
needed repeatedly.
