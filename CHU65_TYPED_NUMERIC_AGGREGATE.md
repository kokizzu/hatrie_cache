# CHU65: Typed Numeric Aggregate Kernel

## Status

Adopted automatically for validated packed `int64` and `float64` columns used
by direct SQL `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` aggregate state. The
aggregate loop reads fixed-width words and validity bits directly instead of
boxing each value through the generic column accessor.

The optimization is per aggregate field. Plain columns, dictionaries, missing
fields, and malformed numeric layouts retain the established value path. SQL
predicate filtering, NULL handling, result limits, and aggregate output types
are unchanged.

## Semantics And Cost

- Numeric `int64` values use the same float conversion as the existing
  aggregate evaluator.
- NULL values are skipped for `SUM`, `AVG`, `MIN`, and `MAX`, and excluded from
  `COUNT(field)`.
- `COUNT(*)` remains a row count and can still use its existing metadata path.
- No per-row state or retained index is added; each aggregate stores only a
  validated fixed-width column view while the query runs.
- Unsupported layouts fail closed to the general executor.

## Benchmark

Workload: `SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM
CACHE('items')` over 100,000 packed `float64` rows with 20% NULL values. Five
runs, `-benchmem`, AMD Ryzen 9 5950X. Lower is better.

| Version | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Before | 19,108,396 | 2,570,263 | 320,035 |
| After | 7,497,930 | 10,510 | 33 |
| Improvement | 2.55x faster | 244.55x less heap | 9,698.03x fewer |

Raw runs:

```text
before: 20,081,627  19,108,396  19,085,368  19,352,525  18,777,893 ns/op
after:   8,177,806   8,109,897   7,089,845   7,426,704   7,497,930 ns/op
```

The feature keeps the generic path for non-packed columns, so the measured
gain is isolated to sources that already paid the cost of validated typed
column storage.

## Verification

```text
make format-chu65-typed-numeric-aggregate
make test-chu65-typed-numeric-aggregate
make benchmark-chu65-typed-numeric-aggregate
make race-chu65-typed-numeric-aggregate
make vet-chu65-typed-numeric-aggregate
```
