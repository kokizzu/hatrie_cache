# CH-030a Prepared SQL/JSON Path Programs

This is a small ClickHouse-inspired query-planning improvement for the larger
CH-030 nested map/subcolumn work. A SQL/JSON path written as a literal is
parsed once when the SQL expression is bound, then reused for every row.

## Scope

- `JSON_VALUE`, `JSON_QUERY`, and `JSON_EXISTS` with a literal path use an
  immutable compiled path program.
- A path supplied through a bound parameter is compiled after binding.
- A path supplied by a row field or another expression is still parsed at
  runtime.
- Invalid literal paths produce the same evaluation error as before.
- Public `JSONPathValue` behavior, wire formats, storage formats, and query
  defaults are unchanged.

This avoids retaining decoded JSON or adding a process-wide unbounded path
cache. The compiled program lives with the prepared SQL expression and is
shared safely by cloned query plans.

## Measurement

Command:

```text
make benchmark-ch030-json-path-program
```

Linux `amd64`, Go `-32` benchmark workers, AMD Ryzen 9 5950X, five samples
per case. The benchmark evaluates the same parsed `JSON_VALUE` expression
before and after path preparation over a decoded nested object.

| Variant | Median ns/op | B/op | Allocs/op | CPU improvement | Allocation improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before preparation | 166.7 | 176 | 2 | 1.00x | 1.00x |
| Prepared literal path | 124.9 | 112 | 1 | 1.33x | 1.57x |

The prepared path is 25.1% shorter in this CPU-bound path lookup, uses 36.4%
fewer bytes, and removes one allocation. JSON text decoding remains the
dominant cost for string or byte inputs; this change does not retain decoded
documents or change their semantics.

### Raw samples

```text
# Before preparation
BenchmarkCH030JSONPathLiteralEvaluation-32    6978637  168.5 ns/op  176 B/op  2 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    7204611  164.4 ns/op  176 B/op  2 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    7388232  166.1 ns/op  176 B/op  2 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    7307899  166.7 ns/op  176 B/op  2 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    7023122  173.0 ns/op  176 B/op  2 allocs/op

# Prepared literal path
BenchmarkCH030JSONPathLiteralEvaluation-32    9510499  128.0 ns/op  112 B/op  1 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    9457512  124.0 ns/op  112 B/op  1 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    9612483  125.8 ns/op  112 B/op  1 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    9243722  124.9 ns/op  112 B/op  1 allocs/op
BenchmarkCH030JSONPathLiteralEvaluation-32    9426471  125.7 ns/op  112 B/op  1 allocs/op
```

## Verification

```text
make test-ch030-json-path-program
make test-sql-json-paths
make test-sql-extensions
make race-ch030-json-path-program
make vet-ch030-json-path-program
```
