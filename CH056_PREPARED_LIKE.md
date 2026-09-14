# CH-056 Prepared Literal `LIKE` Patterns

## Decision

Adopt bind-time preparation for literal string `LIKE` patterns. The query
binder splits the pattern on `%` once and keeps the immutable parts on the
expression. Scalar, batch, dictionary, vector, and columnar stream paths use
the same prepared matcher. Dynamic patterns continue to use the existing
runtime path.

Binary-collation execution uses the prepared parts directly. Other collations
fall back to the existing collation-aware matcher so matching semantics stay
centralized. Raw byte-sensitive NGram pruning is disabled for non-binary
collations; it could otherwise skip a segment that contains a case variant of
the search text.

The preparation is limited to string literals, including values substituted
for bound parameters. Non-string literals retain the existing `fmt.Sprint`
behavior. `NULL` operands retain SQL `NULL` behavior.

## Tradeoff

Each prepared expression retains one pattern string and its percent-separated
parts for the lifetime of the prepared query. This is a small one-time bind
cost and avoids splitting the same pattern and allocating conversion work for
every row. Dynamic patterns have no additional retained program.

## Verification

Focused tests cover literal and dynamic patterns, bound parameters, `NULL`,
binary and Unicode case-insensitive collation, wildcard behavior already
supported by this SQL dialect, columnar matching, dictionary/vector paths, and
NGram false-negative protection.

## Benchmark

Command:

```text
make benchmark-ch056-like
```

The benchmark runs ten samples of the same scalar expression on an AMD Ryzen
9 5950X. The baseline disables the prepared program; the prepared case binds
the literal pattern once before the timed loop.

| Case | Median ns/op | Memory | Relative result |
| --- | ---: | ---: | ---: |
| Baseline matcher | 230.45 | 80 B/op, 3 allocs/op | 1.00x |
| Prepared matcher | 48.79 | 0 B/op, 0 allocs/op | 4.72x faster |

The result is CPU-only evaluation. It does not claim a reduction in the
one-time query preparation cost or in the retained bytes of a prepared query.

Raw baseline samples:

```text
BenchmarkCH056LiteralLike/baseline-32  5368688  223.6 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5372401  225.0 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5226055  229.2 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5170363  227.8 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5235564  231.7 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5310283  226.5 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  4999038  241.5 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  5290582  251.2 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  4898895  250.4 ns/op  80 B/op  3 allocs/op
BenchmarkCH056LiteralLike/baseline-32  4779208  253.2 ns/op  80 B/op  3 allocs/op
```

Raw prepared samples:

```text
BenchmarkCH056LiteralLike/prepared-32  24872214  50.18 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  23798206  50.19 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  25149020  53.60 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  24305446  47.98 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  21113809  47.39 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  23239536  47.52 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  24874580  48.32 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  20743996  51.41 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  23270899  49.26 ns/op  0 B/op  0 allocs/op
BenchmarkCH056LiteralLike/prepared-32  24853663  45.94 ns/op  0 B/op  0 allocs/op
```
