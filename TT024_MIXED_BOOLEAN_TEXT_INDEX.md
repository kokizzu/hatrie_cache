# TT-024 Mixed Boolean Text Index Planning

Status: adopted for same-field `CONTAINS_PHRASE` and
`CONTAINS_PROXIMITY` predicates inside a mixed Boolean `OR`.

The planner now narrows expressions such as:

```sql
WHERE (CONTAINS_PHRASE(doc.text, 'alpha beta') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.text, 'gamma delta', 1) AND doc.kind = 'b')
```

Each `OR` branch must contain an indexable phrase/proximity predicate on the
same field. The existing source-owned union resolver performs identity-aware,
source-order candidate unioning. The SQL executor still evaluates the full
Boolean expression after the candidate lookup, so the index is only a work
reduction and cannot change results or multiplicity.

Cross-field expressions, `OR` branches without an indexable text predicate,
and unsupported Boolean operators retain the full-scan path.

## Verification

Commands:

```text
make format-tt024-text
make test-tt024-text
make race-tt024-text
make vet-tt024-text
make test-tt024-package
make benchmark-tt024-text
```

The focused regression tests cover indexed mixed branches, an uncovered
branch, and cross-field fallback. The race, vet, and complete `hatSql`
package checks passed.

## Benchmark

Raw command:

```text
make benchmark-tt024-text
```

Machine: Linux amd64, AMD Ryzen 9 5950X. Each case uses the same 50,000-row
fixture and five benchmark samples. The indexed candidate subset is about 500
rows; index construction is intentionally outside the timed query path.

Raw output from the final run:

```text
BenchmarkTT024MixedBooleanFullScan-32            21  52496555 ns/op  59075436 B/op  601075 allocs/op
BenchmarkTT024MixedBooleanFullScan-32            20  51853130 ns/op  59075454 B/op  601075 allocs/op
BenchmarkTT024MixedBooleanFullScan-32            20  52566174 ns/op  59075461 B/op  601075 allocs/op
BenchmarkTT024MixedBooleanFullScan-32            19  53485501 ns/op  59075436 B/op  601075 allocs/op
BenchmarkTT024MixedBooleanFullScan-32            21  51561436 ns/op  59075433 B/op  601075 allocs/op
BenchmarkTT024MixedBooleanIndexedUnion-32      1965    558328 ns/op    667455 B/op    6028 allocs/op
BenchmarkTT024MixedBooleanIndexedUnion-32      1819    564475 ns/op    667456 B/op    6028 allocs/op
BenchmarkTT024MixedBooleanIndexedUnion-32      1886    551058 ns/op    667456 B/op    6028 allocs/op
BenchmarkTT024MixedBooleanIndexedUnion-32      1852    558323 ns/op    667453 B/op    6028 allocs/op
BenchmarkTT024MixedBooleanIndexedUnion-32      1836    557741 ns/op    667454 B/op    6028 allocs/op
BenchmarkTT024MixedBooleanFullScanFallback-32    31  37145020 ns/op  48888755 B/op  350557 allocs/op
BenchmarkTT024MixedBooleanFullScanFallback-32    31  37573591 ns/op  48888749 B/op  350557 allocs/op
BenchmarkTT024MixedBooleanFullScanFallback-32    28  39052902 ns/op  48888855 B/op  350556 allocs/op
BenchmarkTT024MixedBooleanFullScanFallback-32    31  37956946 ns/op  48888735 B/op  350556 allocs/op
BenchmarkTT024MixedBooleanFullScanFallback-32    28  37597515 ns/op  48888541 B/op  350556 allocs/op
BenchmarkTT024MixedBooleanIndexedFallback-32     31  36598475 ns/op  48888814 B/op  350557 allocs/op
BenchmarkTT024MixedBooleanIndexedFallback-32     31  38302513 ns/op  48888808 B/op  350557 allocs/op
BenchmarkTT024MixedBooleanIndexedFallback-32     30  36816936 ns/op  48888643 B/op  350557 allocs/op
BenchmarkTT024MixedBooleanIndexedFallback-32     26  38581247 ns/op  48888997 B/op  350558 allocs/op
BenchmarkTT024MixedBooleanIndexedFallback-32     31  39112579 ns/op  48888846 B/op  350558 allocs/op
```

Median comparison:

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Full scan | 52,496,555 | 59,075,436 | 601,075 | baseline |
| Mixed Boolean indexed union | 558,323 | 667,455 | 6,028 | 94.0x faster, 88.5x lower bytes, 99.7x fewer allocations |
| Full-scan fallback | 37,597,515 | 48,888,749 | 350,556 | fallback baseline |
| Indexed resolver, unsupported branch | 38,302,513 | 48,888,814 | 350,557 | 1.9% slower, effectively equal memory |

The selective path is a large win. The measured unsupported-branch cost is a
small CPU regression with one additional allocation, so the conservative
fallback was retained instead of forcing an index lookup.
