# M217 Complete-Row Point-Lookup Index

M217 adds `hatSql.IncrementalPointLookup`, an importable arrangement for a
maintained view whose hot read is equality lookup by one derived key. It
retains each active complete row once and keeps a posting bucket from the
lookup value to that row. The arrangement is opt-in; existing SQL scans,
resolver interfaces, and defaults are unchanged.

## Example

```go
index, err := hatSql.NewIncrementalPointLookup(
	hatSql.IncrementalPointLookupDefinition{
		IndexKey: func(row hatSql.Row) (string, error) {
			return row["region"].(string), nil
		},
	},
)
if err != nil {
	return err
}

err = index.Apply([]hatSql.DifferentialRow{
	{
		Key:  "account-1",
		Time: 10,
		Diff: 1,
		Row:  hatSql.Row{"id": "account-1", "region": "ap-southeast", "tier": "pro"},
	},
})
if err != nil {
	return err
}

rows := index.Lookup("ap-southeast")
```

`Lookup` returns complete `DifferentialRow` values sorted by stable row key.
`Diff` is the current positive multiplicity. `Snapshot` returns all active
rows, and `Len`/`BucketCount` expose bounded state counts without exposing row
contents.

## Update Contract

- A positive update for a new row key requires `Row`; the index-key callback
  derives its posting value.
- A positive update for an existing key may omit `Row` to add multiplicity.
  If `Row` is supplied, it must match the retained complete row and index key.
- A negative update needs only the stable row key. Underflow, overflow, empty
  row keys, callback errors, and row conflicts reject the whole batch.
- Moving a row between point-lookup keys is represented by a negative update
  followed by a positive update with the new row image.
- Inserted rows and lookup/snapshot results are cloned. Callers can mutate
  their own maps without changing the arrangement.
- The index uses a mutex for concurrent reads and writes. The key callback
  must not call back into the same index while `Apply` is running.

The implementation stores one row map per active stable row key, not one copy
per multiplicity. The posting bucket points to that retained row, so repeated
duplicates increase the signed count without multiplying row storage.

## Measurement

Linux `amd64`, AMD Ryzen 9 5950X, `go test -benchmem -count=5 -benchtime=200ms`.
The fixture has 10,000 complete rows, 64 point-lookup keys, and 156 matching
rows for `team-42`. The scan baseline clones matching rows while checking all
10,000 source rows. The maintained path builds the index outside the timed
lookup loop and clones the same 156 complete rows on each read.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Full scan point lookup | 283,819 | 53,697 | 313 | 1.00x |
| Maintained complete-row lookup | 100,450 | 61,632 | 314 | 2.83x faster; 14.8% more B/op; +1 alloc |

Index construction for the same 10,000 rows measured 13,572,911 ns/op,
7,467,850 B/op, and 35,938 allocations/op. That cost is setup/maintenance,
not part of each lookup. The arrangement is a good fit for repeated hot
lookups; a low-read workload should keep using the scan path rather than pay
the retained-row and posting-map cost.

Raw benchmark samples:

```text
BenchmarkM217FullScanPointLookup-32           978 288308 ns/op 156.0 result_rows 10000 source_rows 53703 B/op 313 allocs/op
BenchmarkM217FullScanPointLookup-32           825 270726 ns/op 156.0 result_rows 10000 source_rows 53697 B/op 313 allocs/op
BenchmarkM217FullScanPointLookup-32           907 278453 ns/op 156.0 result_rows 10000 source_rows 53704 B/op 313 allocs/op
BenchmarkM217FullScanPointLookup-32           801 285237 ns/op 156.0 result_rows 10000 source_rows 53697 B/op 313 allocs/op
BenchmarkM217FullScanPointLookup-32           765 283819 ns/op 156.0 result_rows 10000 source_rows 53697 B/op 313 allocs/op
BenchmarkM217MaintainedPointLookup-32        2523 100548 ns/op 156.0 result_rows 10000 retained_entries 61634 B/op 314 allocs/op
BenchmarkM217MaintainedPointLookup-32        2120  98937 ns/op 156.0 result_rows 10000 retained_entries 61632 B/op 314 allocs/op
BenchmarkM217MaintainedPointLookup-32        2462  96745 ns/op 156.0 result_rows 10000 retained_entries 61632 B/op 314 allocs/op
BenchmarkM217MaintainedPointLookup-32        2311 100450 ns/op 156.0 result_rows 10000 retained_entries 61632 B/op 314 allocs/op
BenchmarkM217MaintainedPointLookup-32        1996 100634 ns/op 156.0 result_rows 10000 retained_entries 61635 B/op 314 allocs/op
BenchmarkM217MaintainedPointLookupBuild-32     15 14208331 ns/op 10000 retained_entries 7486491 B/op 36270 allocs/op
BenchmarkM217MaintainedPointLookupBuild-32     16 12504068 ns/op 10000 retained_entries 7467850 B/op 35938 allocs/op
BenchmarkM217MaintainedPointLookupBuild-32     15 14181010 ns/op 10000 retained_entries 7486508 B/op 36270 allocs/op
BenchmarkM217MaintainedPointLookupBuild-32     16 13572911 ns/op 10000 retained_entries 7467847 B/op 35938 allocs/op
BenchmarkM217MaintainedPointLookupBuild-32     18 13324466 ns/op 10000 retained_entries 7436705 B/op 35385 allocs/op
```

## Verification

The focused tests cover multiplicity, complete-row retention, key movement,
clone isolation, empty posting keys, row conflicts, underflow, and atomic
failure. Verification runs:

```text
make test-m217-red
make test-m217-green
make race-m217
make benchmark-m217
make verify-m217
```
