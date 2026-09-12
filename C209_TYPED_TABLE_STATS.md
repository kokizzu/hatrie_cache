# C209: Typed-Table Column Statistics

`TypedTable.Stats()` provides an exact, schema-ordered snapshot of active rows
without requiring callers to maintain metadata. It reports table row count and
per-column NULL count, value count, and scalar minimum/maximum values.

```go
stats := table.Stats()
for _, column := range stats.Columns {
	log.Printf("%s rows=%d nulls=%d min=%v max=%v",
		column.Name, stats.RowCount, column.NullCount, column.Min, column.Max)
}
```

## Semantics

- Deleted patch-part rows are excluded from `RowCount` and every column count.
- NULL values increment `NullCount` and never participate in min/max.
- Float NaN values increment `ValueCount` but are excluded from min/max.
- String, int64, float64, and bool columns expose min/max using their native
  typed value. A column with no comparable non-NULL value has `HasMinMax=false`.
- Columns are returned in schema order, and the returned values do not alias
  table storage.
- The nil receiver returns the zero-value `TypedTableStats`.

The snapshot scans existing typed column storage under the table read lock. It
does not add per-write bookkeeping, duplicate min/max maps, or retained state;
the default SQL execution path is unchanged unless a caller explicitly asks
for statistics.

## Benchmark

Five samples, Go benchmark `-benchmem`, AMD Ryzen 9 5950X. The first comparison
uses the same 10,000-row typed table with four columns and compares the stats
snapshot with the existing columnar source materialization.

| Workload | Median ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| `TypedTable.Stats()` | 810,177 | 640 | 1 | baseline |
| Existing columnar materialization | 996,471 | 1,020,138 | 29,768 | stats is 1.23x faster, 1,594x lower B/op, 29,768x fewer allocations |

The existing 100,000-row SQL min/max benchmark was also rerun to detect a
regression in nearby behavior. The legacy row-scan median changed from
5,973,013 to 5,788,670 ns/op; the segment-metadata median changed from 7,817
to 7,748 ns/op. Both retained 5,568 B/op and 23 allocs/op. These small changes
are within normal run variance and show no allocation regression.

Raw final samples:

```text
TypedTable.Stats(): 810177, 759974, 754241, 838865, 826469 ns/op
Columnar materialization: 1059249, 996471, 985230, 1078862, 990233 ns/op
```

Verification:

```text
make verify-c209
make benchmark-c209
```
