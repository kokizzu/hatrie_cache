# C210: Compact Typed-Table Histograms

`TypedTable.Histogram()` builds a bounded numeric distribution snapshot for
`int64` and `float64` columns. The bins retain exact counts while their fixed
number keeps the result compact for selectivity and range-estimate consumers.

```go
histogram, err := table.Histogram("score", hatSql.TypedTableHistogramOptions{
	Bins: 32,
})
if err != nil {
	return err
}
for _, bin := range histogram.Bins {
	log.Printf("[%v, %v] count=%d", bin.Lower, bin.Upper, bin.Count)
}
```

## Semantics

- Zero `Bins` uses `DefaultTypedTableHistogramBins` (`32`); the maximum is
  `MaxTypedTableHistogramBins` (`256`). A constant column is represented by
  one bin, and an integer range smaller than the requested count uses fewer
  bins.
- `RowCount` includes active rows, while `NullCount` and `ValueCount` are
  exact. Deleted patch-part rows are excluded.
- `Bins` contain finite non-NULL values. Float NaN and infinity values remain
  exact in `ValueCount` and are reported separately by `UnbucketedCount`.
- Integer bounds are exact and safe across the complete signed `int64` range.
  Float bins use lower-inclusive, upper-exclusive ranges except for the final
  bin, which includes its maximum.
- String and bool histograms are rejected explicitly because a numeric
  fixed-width range histogram would give misleading selectivity estimates for
  those types.
- The first request for a field and normalized bin count is computed under the
  table lock and retained as a bounded snapshot. Repeated requests clone the
  bins without rescanning rows. Successful `Upsert` and `Delete` invalidate
  all histogram snapshots; physical patch compaction preserves them because
  active rows and values do not change. At most eight field/bin variants are
  retained per table.

## Benchmark

Five samples, Go benchmark `-benchmem`, AMD Ryzen 9 5950X, using a 10,000-row
typed table. The C209 stats control remained allocation-stable after adding the
histogram API.

| Workload | Median ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| C209 `TypedTable.Stats()` before C210 | 843,252 | 640 | 1 | four-column stats control |
| C209 `TypedTable.Stats()` after C210 | 831,005 | 640 | 1 | 1.01x versus the control; within run variance |
| Uncached `TypedTable.Histogram("score", Bins: 32)` (before) | 90,188 | 3,456 | 1 | one numeric column, two scans, bounded 32-bin result |
| Cached `TypedTable.Histogram("score", Bins: 32)` (after) | 622.1 | 3,456 | 1 | 145x faster; equal transient bytes and allocations |

Each cached 32-bin result retains roughly 3.4 KiB plus map overhead and does
not retain table rows; the eight-entry cap bounds the retained histogram
payload. A cold request remains proportional to the selected column's active
rows, with a second pass used to fill exact bin counts.

Raw final samples:

```text
TypedTable.Stats(): 831005, 866445, 848411, 819182, 747001 ns/op
TypedTable.Histogram() before: 90188, 90647, 89674, 90261, 89860 ns/op
TypedTable.Histogram() after: 620.7, 583.9, 622.1, 693.1, 649.4 ns/op
```

Verification:

```text
make verify-c210
make benchmark-c210
```
