# CH-048 Packed Numeric `IN` Predicate Kernels

## Scope

Direct predicates of the form `field IN (literal, ...)` can use a packed
columnar kernel when all of the following are true:

- the field resolves to a packed `int64` or `float64` column;
- every list item is a non-`NULL` numeric literal;
- integer columns receive exact integer-valued literals; and
- the packed column metadata and row count validate successfully.

The kernel sorts and deduplicates the literal set once, then decodes one
fixed-width value per row and uses binary search. Validity bits are checked
before the value bytes, so nullable packed columns preserve SQL `NULL`
semantics.

`NOT IN`, lists containing `NULL`, dynamic expressions, unsafe integer/float
coercions, malformed packed data, unsupported column kinds, and non-columnar
inputs use the existing general evaluator. This keeps the optimization
additive and preserves the established three-valued SQL behavior.

## Verification

Focused tests cover recognizer guards, integer and float columns, nullable
validity bitmaps, duplicate literals, malformed metadata, `NOT IN`, and
`IN` lists containing `NULL`:

```text
make format-ch048-numeric-in
make test-ch048-numeric-in
```

The full package, race, vet, broader CH-048, and documentation checks are
provided by the corresponding Makefile targets.

## Benchmark

Five samples per case, 4,096 packed numeric rows, Linux/amd64, AMD Ryzen 9
5950X. The pre-change run is the original general-evaluator binary; the
paired fallback uses the same post-change binary and fixtures to isolate the
predicate kernel.

| Case | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Pre-change general evaluator | 1,003,325 | 555,016 | 12,032 | 1.00x |
| Paired general-evaluator fallback | 967,145 | 555,015 | 12,032 | 1.00x |
| Packed numeric `IN` kernel | 23,331 | 0 | 0 | 41.45x faster than fallback |

The fast path removes 555,015 B/op and 12,032 allocations/op for this
workload. It changes neither the wire format nor persistence format.

Raw output from `make benchmark-ch048-numeric-in`:

```text
BenchmarkCH048NumericINBaseline:
990303 ns/op 555016 B/op 12032 allocs/op
997627 ns/op 555039 B/op 12032 allocs/op
983338 ns/op 555016 B/op 12032 allocs/op
985828 ns/op 555015 B/op 12032 allocs/op
970361 ns/op 555016 B/op 12032 allocs/op

BenchmarkCH048NumericINFallback:
980697 ns/op 555015 B/op 12032 allocs/op
989431 ns/op 555016 B/op 12032 allocs/op
965736 ns/op 555015 B/op 12032 allocs/op
957887 ns/op 555016 B/op 12032 allocs/op
967145 ns/op 555015 B/op 12032 allocs/op

BenchmarkCH048NumericINFastPath:
23536 ns/op 0 B/op 0 allocs/op
23029 ns/op 0 B/op 0 allocs/op
23292 ns/op 0 B/op 0 allocs/op
23331 ns/op 0 B/op 0 allocs/op
23536 ns/op 0 B/op 0 allocs/op
```
