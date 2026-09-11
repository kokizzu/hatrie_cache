# SQL Packed Numeric Predicate Kernel

This is the SQL-side follow-up to the existing `hatPredicate` SIMD mask
package. Columnar SQL scans already store eligible `int64` and `float64`
columns in fixed-width packed bytes. Direct numeric `WHERE` conjunctions now
read those bytes and validity bits through a small prevalidated kernel instead
of boxing every value through `ColumnarBatch.Value` and converting it back with
`sqlNumber`.

The optimization is deliberately conservative. It is used only when every
numeric predicate column is present, packed, and structurally valid. Legacy
interface columns, mixed predicates, malformed batches, NULL rows, and all
other SQL shapes retain the existing evaluator. There is no new configuration
flag, wire format, storage format, or resident mask allocation.

## Semantics

- NULL values do not match any direct comparison, preserving SQL three-valued
  predicate behavior.
- `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=` use the same `float64` comparison
  semantics as the existing evaluator, including NaN and infinity behavior.
- Invalid packed metadata disables the kernel and falls back before scanning.
- Projection still uses the established `ColumnarBatch.Value` path, so output
  types remain unchanged.

## Measurement

Seven `250ms` samples on an AMD Ryzen 9 5950X, linux/amd64, with
`GOMAXPROCS=1`, using `make benchmark-ch048-numeric-predicate-kernel`. The
benchmark builds a 4,096-row typed table and repeatedly executes the same
numeric filter.

| Workload | Before | After | Result |
| --- | ---: | ---: | ---: |
| Packed numeric query | 693,848 ns/op; 826,478 B/op; 10,759 allocs/op | 611,260 ns/op; 808,011 B/op; 8,448 allocs/op | 1.14x faster; 2.2% less heap; 21.5% fewer allocations |
| Legacy-column control | 686,955 ns/op; 791,066 B/op; 6,345 allocs/op | 627,961 ns/op; 791,010 B/op; 6,343 allocs/op | Same allocation profile; CPU variance is expected across separate benchmark processes |

The packed result is a workload-specific CPU improvement. The important
structural result is the allocation reduction; the fallback path remains
available for compatibility and does not retain packed metadata.
