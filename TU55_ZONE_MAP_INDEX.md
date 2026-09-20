# T-U55 Zone-Map Data Skipping Index

## Status

Adopted as the opt-in generic `hatDataStructure.ZoneMapIndex[K]`. It keeps
inclusive min/max bounds for fixed-size row blocks and visits only blocks that
may overlap an equality or inclusive range predicate. The caller still owns
row storage and must verify the predicate on each returned row.

## Inspiration and Boundary

ClickHouse data-skipping indexes use per-granule min/max metadata to avoid
reading blocks that cannot satisfy a predicate. The same bounded summary fits
Materialize-style arranged batches and Tarantool ordered-space scans when data
is physically clustered.

The index is intentionally conservative: it may return extra blocks, but it
never skips a block whose bounds overlap the requested predicate. It does not
sort data, reorder rows, or make assumptions about a caller's storage layout.
Unclustered data can reduce the pruning benefit, so it remains opt-in.

## Example

```go
index, err := hatDataStructure.NewZoneMapIndex(
	64,
	func(left, right uint64) bool { return left < right },
)
if err != nil {
	return err
}
if err := index.Build(values); err != nil {
	return err
}
index.VisitRange(low, high, func(segment hatDataStructure.ZoneMapSegment[uint64]) bool {
	for _, row := range rows[segment.Start:segment.End] {
		// Verify the actual row predicate here.
		_ = row
	}
	return true
})
```

`Build` replaces metadata only after the new segment slice is ready. Segment
size is bounded, empty builds clear the index, and visitor callbacks can stop
scanning early without an allocation.

## Measurement

Five-sample medians on AMD Ryzen 9 5950X, Linux amd64, 1,048,576 clustered
`uint64` rows and segment size 64:

| Measurement | Baseline | Zone map | Difference |
| --- | ---: | ---: | ---: |
| Dense per-row min/max/range metadata build | 7,769,306 ns/op, 33,554,432 B/op | 3,601,323 ns/op, 524,288 B/op | 2.16x faster, 64x less metadata |
| Raw value-copy build | 872,511 ns/op, 8,388,620 B/op | 3,601,323 ns/op, 524,288 B/op | 4.13x slower, 16x less retained data |
| Equality scan, clustered | 525,837 ns/op | 48,959 ns/op | 10.7x faster |
| Equality scan, deterministic scattered | 521,161 ns/op | 393,180 ns/op | 1.33x faster |

The dense metadata comparison measures the index cost directly. The raw-copy
comparison is included to show that bound construction is not free; it is not
an equivalent query index. The query results show why physical clustering and
segment size determine whether the feature pays for itself.
