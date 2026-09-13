# SQL Bitmap Aggregates

The SQL bitmap functions use the existing compressed Roaring implementation
through the exported `SQLBitmap` type.

```sql
SELECT region,
       BITMAP_COUNT(BITMAP_AGG(user_id)) AS unique_users,
       BITMAP_CONTAINS(BITMAP_AGG(user_id), 42) AS contains_user_42
FROM CACHE('events')
GROUP BY region;
```

Supported functions:

| Function | Behavior |
| --- | --- |
| `BITMAP_AGG(value)` | Builds a distinct compressed bitmap from integer values |
| `BITMAP_COUNT(bitmap)` | Returns distinct-member cardinality as `int64` |
| `BITMAP_CONTAINS(bitmap, value)` | Tests membership |
| `BITMAP_OR(left, right)` | Returns the union |
| `BITMAP_AND(left, right)` | Returns the intersection |
| `BITMAP_XOR(left, right)` | Returns the symmetric difference |

Bitmap members must be integer-valued and in the inclusive `uint32` range.
`NULL` values are skipped by `BITMAP_AGG`; a NULL bitmap argument produces NULL
from scalar bitmap functions. Invalid values return an error. Set operations
do not mutate either input bitmap.

`BITMAP_AGG` returns an in-process `SQLBitmap`, which exposes `Add`, `Count`,
`Contains`, `Values`, and `EncodedSize`. JSON encoding uses sorted `uint32`
values so existing clients can consume the result without knowing the compact
internal representation.

## Measurement

The benchmark uses 16,384 rows, 64 groups, and values in a 4,096-member
domain. It ran on Linux/amd64 with an AMD Ryzen 9 5950X, five samples, and
`-benchmem`.

| Workload | Median ns/op | B/op | Allocs/op | Improvement / cost |
| --- | ---: | ---: | ---: | --- |
| Existing `GROUP_UNIQ_ARRAY(value)` | 19,642,776 | 16,125,142 | 82,867 | baseline |
| `BITMAP_COUNT(BITMAP_AGG(value))` | 10,553,468 | 15,521,651 | 83,123 | 1.86x faster, 3.7% lower B/op, 0.3% more allocations |

The bitmap path is a CPU and allocation-byte win for distinct cardinality,
with a small increase in allocation count from constructing compressed bitmap
containers. The result also retains a compressed set rather than a distinct
interface slice, which is useful when the bitmap is reused by membership or
set operations.

A final rerun after the asymmetric-intersection correctness fix paired the
existing control at 21,833,893 ns/op, 16,124,711 B/op, and 82,867 allocs/op
with bitmap cardinality at 10,930,271 ns/op, 15,521,877 B/op, and 83,123
allocs/op: 2.00x faster, 3.7% lower B/op, and 0.3% more allocations.

## Verification

```text
make test-ch038
make benchmark-ch038-after
```
