# TR-026 Typed Bitmap Index

Status: implemented as an opt-in derived index.

`hatDataStructure.BitmapIndex[K comparable]` maps typed values to exact
`uint32` row IDs. It reuses the existing `RoaringBitmap` sparse/dense
containers, so a low-cardinality field does not need one pointer-bearing index
entry per row. Generic keys keep string, integer, and normalized date/time
indexes on the typed path without converting values to `interface{}`.

## Example

```go
index := hatDataStructure.NewBitmapIndex[string]()
for row, status := range statuses {
	index.Add(status, uint32(row))
}

index.Visit("active", func(row uint32) bool {
	// Consume row without allocating a result slice.
	_ = row
	return true
})

matches := index.Intersect("active", "verified")
rows := matches.Values()
```

Use `Add` and `Remove` when a row's indexed value changes. Duplicate adds and
removes are idempotent. `Visit` streams sorted row IDs without allocating;
`Rows` returns an owned slice. `Intersect` and `Union` return an independently
owned `RoaringBitmap` result. Numeric keys such as `uint64` are supported
directly; dates and datetimes should use a canonical integer representation
when equality semantics need to ignore location or formatting.

The zero value is usable. Mutation and reads require external synchronization.
The row ID limit is `uint32`, matching the underlying RoaringBitmap.

## Lifecycle

This index is not enabled automatically and is not part of the default table
storage or wire format. It is derived state: rebuild it from the authoritative
rows after backup restore, and remove/re-add memberships around updates. This
avoids making backups dependent on an index implementation or allowing stale
index state to become authoritative.

Bitmap indexes are a good fit for repeated equality filters and set
intersections on low-cardinality columns. Do not add one to every field:
high-cardinality values, write-heavy workloads, and queries that touch most
rows may be better served by a sorted arrangement or a scan.

## Measurement

Command: `make benchmark-tr026-bitmap-index`. Five samples ran on Linux/amd64
with an AMD Ryzen 9 5950X. The fixture has 100,000 rows and 16 repeating
values. The pre-change scan baseline was captured before the index existed.

| Path | Raw `ns/op` samples | Median | Heap | Allocs | Relative |
| --- | --- | ---: | ---: | ---: | ---: |
| Pre-change linear scan | 47272; 48593; 44009; 45626; 48516 | 47272 | 0 B/op | 0 | 1.00x |
| Bitmap `Visit` lookup | 12328; 11663; 11942; 11648; 11226 | 11663 | 0 B/op | 0 | 4.05x faster |

The bitmap lookup samples are `12328; 11663; 11942; 11648; 11226`; the table
also shows the post-change linear samples to expose host variance. The
steady-state bitmap query reports no heap allocation. The bitmap build reports
`672297 B/op` and `440 allocs/op` while constructing the derived index, versus
`106496 B/op` and `1 alloc/op` to copy the raw fixture. Its measured
`bitmap-bytes` footprint is `200000` for the 100,000-row, 16-value fixture.
Construction is therefore intentionally an amortized cost, and the index is
opt-in for read-heavy workloads. Wire bandwidth is unchanged.
