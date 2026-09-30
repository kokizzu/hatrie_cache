# CH-051: Composite columnar radix order

Hatrie SQL now uses a stable multi-key radix sort for large homogeneous
columnar `int64` order projections. The implementation applies the least
significant order key first and the most significant key last, preserving the
existing stable ordinal semantics while supporting independent ascending and
descending directions.

The fast path requires at least 256 rows, two or more non-empty order fields,
and an exact `int64` value in every requested column. It materializes compact
field-major numeric values and reuses one row-index scratch buffer across all
eight radix passes for every key. Small batches, mixed numeric types, strings,
missing values, and unsupported values retain the existing generic comparator
path.

The implementation is based on the same typed, column-oriented execution idea
used by ClickHouse: choose a representation-specific ordering kernel when the
column layout proves that it is safe, while keeping a general fallback for
other SQL values.

## Measurement

The benchmark sorts 100,000 rows by `score DESC, id ASC` and samples each
implementation five times with:

```text
make benchmark-round15-composite-order
```

| Metric | Generic comparator | Composite radix | Improvement |
| --- | ---: | ---: | ---: |
| CPU | 22,669,374 ns/op | 6,927,565 ns/op | 3.27x faster |
| Heap | 6,807,720 B/op | 2,408,483 B/op | 2.83x lower |
| Allocations | 8 allocs/op | 4 allocs/op | 2.00x fewer |

The benchmark is an optimization measurement, not a promise that every
`ORDER BY` query takes this path. The generic comparator remains the fallback
for all inputs that do not satisfy the admission rules.
