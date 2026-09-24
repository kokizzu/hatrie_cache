# M046: Typed JSON Subcolumn Top-N

M046 adopts a narrow ClickHouse-style late-materialization path for bounded
`ORDER BY ... LIMIT` queries. The resolver supplies a typed scalar JSON
subcolumn for the sort key, the executor keeps only the best `LIMIT + OFFSET`
rows in the existing Top-N heap, and JSON projections are materialized only for
the final page.

## Scope and fallback

The optimized path is intentionally conservative:

- one source from `CACHE`;
- exactly one `ORDER BY` expression;
- the expression is scalar `JSON_VALUE` with a literal path;
- a finite `LIMIT`; `OFFSET` is supported;
- the requested typed JSON subcolumn is available and row-aligned.

Joins, grouping, aggregates, windows, multiple sort keys, `JSON_QUERY` or
`JSON_EXISTS` sort expressions, unbounded limits, `LIMIT BY`, `WITH FILL`,
malformed or unavailable subcolumns, and other unsupported shapes retain the
existing row-source JSON evaluator. This keeps the default behavior and
correctness contract unchanged for every query outside the narrow fast path.

## Correctness

The optimized executor reuses the existing SQL ordering comparator, including
NULL ordering and explicit NULL direction. It keeps the input ordinal as a
stable tie-breaker, evaluates the existing `WHERE` expression before inserting
rows into the heap, applies `OFFSET` after sorting the retained rows, and
evaluates the original projection against the typed batch. Focused tests cover
descending order, missing and JSON-null values, ties, offsets, and fallback
when the ordered subcolumn is unavailable.

## Benchmark

The benchmark uses 4,096 deterministic rows and returns the top 32 values. Five
`-benchmem` samples were collected with `make benchmark-m046-json-topn` on the
same local Linux `amd64` host. The pre-change candidate is the exact query
before M046, where the candidate resolver was present but the ordered query
fell back to row JSON parsing. The after-build legacy control is run in the
same process as the optimized candidate.

| Variant | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Pre-change candidate, row JSON fallback | 122,176,960 | 83,861,720 | 1,170,700 | 1.00x |
| After-build legacy fallback control | 120,658,857 | 83,862,092 | 1,171,711 | 1.00x control |
| M046 typed JSON Top-N | 1,714,788 | 57,728 | 4,075 | 70.4x faster, 1,454x lower heap, 288x fewer allocations |

The small difference between the two fallback measurements is normal benchmark
noise; the same-run control confirms that the gain comes from the typed ordered
path rather than a change to the row evaluator. The optimization retains only
the bounded heap and final page, so its working memory is proportional to the
requested page rather than all parsed JSON values.

## Verification

The following repository targets passed after implementation:

```text
make test-m046-json-topn
make test-m046-json-regression
make race-m046-json-topn
make vet-m046-json-topn
make test-m046-package
```

Raw benchmark output:

```text
BenchmarkM046JSONSubcolumnTopN/legacy-32         9  117805624 ns/op  83867588 B/op  1171722 allocs/op
BenchmarkM046JSONSubcolumnTopN/legacy-32         9  117952388 ns/op  83862080 B/op  1171711 allocs/op
BenchmarkM046JSONSubcolumnTopN/legacy-32         9  122803411 ns/op  83862108 B/op  1171711 allocs/op
BenchmarkM046JSONSubcolumnTopN/legacy-32         9  120658857 ns/op  83862029 B/op  1171710 allocs/op
BenchmarkM046JSONSubcolumnTopN/legacy-32         9  123515940 ns/op  83862092 B/op  1171711 allocs/op
BenchmarkM046JSONSubcolumnTopN/candidate-32    685  1739031 ns/op    57729 B/op     4075 allocs/op
BenchmarkM046JSONSubcolumnTopN/candidate-32    674  1673764 ns/op    57728 B/op     4075 allocs/op
BenchmarkM046JSONSubcolumnTopN/candidate-32    650  1726758 ns/op    57728 B/op     4075 allocs/op
BenchmarkM046JSONSubcolumnTopN/candidate-32    720  1714788 ns/op    57728 B/op     4075 allocs/op
BenchmarkM046JSONSubcolumnTopN/candidate-32    700  1705018 ns/op    57728 B/op     4075 allocs/op
```
