# T221: Typed BITSET/Bitmap Index

`BitmapIndex[K]` maps a comparable low-cardinality value to a Roaring bitmap
of `uint32` row IDs. It is derived state for membership predicates such as
`status = 'active'` or `region IN (...)`; callers rebuild it from the
authoritative table after restore.

## API

```go
index := NewBitmapIndex[string]()
index.Add("active", 42)
index.Add("active", 43)

index.Contains("active", 42) // true
ids := index.Rows("active")  // [42 43]
```

`Add`, `Remove`, `Contains`, `Rows`, `Visit`, `ValueCount`, `IndexedRows`, and
`Info` manage individual memberships and metadata. `Intersect` returns rows
matching every supplied value; `Union` returns rows matching at least one.
`Visit` streams sorted row IDs through a callback and can stop without building
a result slice. `Rows`, `Intersect`, and `Union` return owned bitmap/slice
results that callers may modify.

The zero value works, keys are generic comparable Go values, and the index is
intentionally not safe for concurrent mutation. Protect it externally or
rebuild a private derived index per reader.

## Cost Model

- Membership uses compact Roaring containers, choosing sparse arrays or dense
  bitsets according to row distribution.
- Low-cardinality repeated predicates avoid scanning every row and can stream
  results without query allocations.
- The build and write path retains one bitmap structure per distinct value, so
  high-cardinality or write-heavy columns can cost more memory and CPU than a
  direct scan.
- The wire and authoritative table formats are unchanged; the index is
  disposable derived state.

## Benchmark

The current benchmark uses 100,000 rows and 16 repeating values. It compares a
linear scan with `BitmapIndex.Visit`, plus the raw-column and one-time-build
controls. Each row has five samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | Median | Memory | Relative |
| --- | ---: | ---: | ---: |
| Linear scan | 49,225 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| Bitmap `Visit` | 11,983 ns/op | 0 B/op, 0 allocs/op | **4.11x faster** |
| Bitmap build | 5,477,014 ns/op | 672,300 B/op, 440 allocs/op | one-time cost |

The build retains `200,000` bitmap bytes for this fixture. That cost is why
the index remains opt-in instead of being created for every field.

Raw current samples from `make benchmark-t221` are:

```text
Linear scan:   55997, 49225, 48315, 51588, 48909 ns/op; 0 B/op; 0 allocs/op
Bitmap Visit:  12537, 12150, 11869, 11879, 11983 ns/op; 0 B/op; 0 allocs/op
Bitmap build: 5474809, 5477014, 5434687, 5741687, 5978742 ns/op; 672300 B/op; 440 allocs/op
```
