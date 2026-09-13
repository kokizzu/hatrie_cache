# CH-051 Prepared SQL Regex Programs

This ClickHouse-inspired expression-planning improvement compiles a literal
regular-expression pattern once when a SQL expression is bound. Row-mode
evaluation reuses the immutable program instead of calling `regexp.Compile`
for every row.

## Scope

- `REGEXP_LIKE` and `REGEXP_EXTRACT` reuse literal patterns.
- Literal `REGEXP` and `NOT REGEXP` predicates reuse their patterns in the
  general row evaluator and batch evaluator.
- The columnar regex path also reuses the prepared program when available.
- Dynamic patterns continue to compile at runtime.
- Invalid literal patterns preserve evaluation-time errors.
- No storage, wire, default configuration, or result semantics change.

The compiled program is immutable and safe to share through cloned prepared
queries. The only retained overhead is one pointer and the compiled regexp
for expressions that use a literal pattern; decoded rows are not retained.

## Measurement

Command:

```text
make benchmark-ch051-regex-program
```

Linux `amd64`, Go `-32` benchmark workers, AMD Ryzen 9 5950X, five samples
per case. The benchmark evaluates a parsed expression repeatedly over one
text row.

| Workload | Variant | Median ns/op | B/op | Allocs/op | CPU improvement | Allocation improvement |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `REGEXP_LIKE` | Before preparation | 3,158 | 4,071 | 50 | 1.00x | 1.00x |
| `REGEXP_LIKE` | Prepared literal | 188.1 | 112 | 1 | 16.8x | 50.0x |
| `REGEXP` predicate | Before preparation | 3,135 | 4,071 | 50 | 1.00x | 1.00x |
| `REGEXP` predicate | Prepared literal | 188.8 | 112 | 1 | 16.6x | 50.0x |

Prepared literals are about 94% shorter, use 97.2% fewer bytes, and remove
49 allocations per evaluation in this workload. Dynamic patterns retain the
old runtime compilation cost because their pattern is not known at bind time.

### Raw samples

```text
# Before preparation
BenchmarkCH051RegexLiteralEvaluation/function-32       349624  3196 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32       368838  3196 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32       339669  3140 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32       368295  3118 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32       356827  3158 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32      364812  3109 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32      378868  3058 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32      386116  3173 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32      389628  3223 ns/op  4071 B/op  50 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32      350364  3135 ns/op  4071 B/op  50 allocs/op

# Prepared literal
BenchmarkCH051RegexLiteralEvaluation/function-32      5687180  200.1 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32      6253912  197.6 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32      6007144  186.1 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32      6547946  183.9 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexLiteralEvaluation/function-32      6491216  188.1 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32     6422198  187.2 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32     6086875  192.8 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32     6181425  192.4 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32     6129730  188.8 ns/op  112 B/op  1 allocs/op
BenchmarkCH051RegexPredicateLiteralEvaluation-32     6554986  181.2 ns/op  112 B/op  1 allocs/op
```

## Verification

```text
make test-ch051-regex-program
make test-sql-regex
make test-sql-extensions
make race-ch051-regex-program
make vet-ch051-regex-program
```
