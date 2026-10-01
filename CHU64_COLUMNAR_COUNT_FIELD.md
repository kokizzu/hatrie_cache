# CHU64: Columnar `COUNT(field)` Metadata

## Status

Adopted automatically for direct, unfiltered `COUNT(field)` aggregates when
the requested field is stored as a validated fixed-width numeric column,
bit-packed boolean column, or dense nullable packed column.

The executor counts validity bits or dense non-NULL values without decoding
one value per row. Plain columns, dictionaries without NULL metadata, missing
fields, and malformed layouts retain the existing row-value path. A `WHERE`
clause also retains the existing filtered aggregate path because the count
must be restricted to matching rows.

## Semantics

- `COUNT(field)` counts only non-NULL values.
- `COUNT(*)` keeps its existing row-count metadata path.
- Mixed no-filter `COUNT(*)`, `COUNT(field)`, and `SUM` queries reuse the
  field count when safe while still scanning for the other aggregate.
- Unknown or invalid physical layouts fail closed to the established executor.
- The feature has no configuration flag and adds no retained state.

## Benchmark

Workload: `SELECT COUNT(value) AS total FROM CACHE('items')` over 100,000
validated packed `int64` rows, with 20% NULL values. Five benchmark runs,
`-benchmem`, AMD Ryzen 9 5950X.

| Version | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Before | 5,125,712 | 643,212 | 79,817 |
| After | 11,833 | 4,816 | 20 |
| Improvement | 433.17x faster | 133.56x less heap | 3,990.85x fewer |

Raw runs:

```text
before: 5,016,074  5,033,377  5,184,494  5,125,712  5,134,833 ns/op
after:     11,691     11,414     11,833     12,105     12,024 ns/op
```

The improvement is largest for a direct unfiltered count because the entire
result is already represented by physical validity metadata. Filtered counts
remain on the scan path by design.

## Verification

```text
make format-chu64-count-field
make test-chu64-count-field
make benchmark-chu64-count-field
make race-chu64-count-field
make vet-chu64-count-field
```
