# M037k Differential String MIN/MAX

`GroupMinMaxStringDifferentialRows` extends the Materialize-style signed
differential aggregate surface to lexicographically ordered string values.

Each group retains the multiplicity of every string value. Positive and
negative `Diff` updates therefore support exact retractions: removing the
current minimum or maximum scans only that group's live value map and restores
the next endpoint. Duplicate-only updates emit no visible aggregate change.

The API is opt-in and callback-based:

```go
updates, err := hatSql.GroupMinMaxStringDifferentialRows(
    rows,
    func(row hatSql.SQLRow) string { return row["group"].(string) },
    func(row hatSql.SQLRow) (string, error) { return row["value"].(string), nil },
)
```

The output rows contain `min` and `max` strings. Empty strings are valid
values. Invalid negative group counts, value multiplicities, checked overflow,
and callback errors return no partial output. Existing integer differential
functions and all default SQL execution paths are unchanged.

## Measurement

`make benchmark-differential-group-min-max` runs five samples on one CPU with
2,048 updates across 256 groups. The paired naive implementation rescans all
live values after every update.

| workload | median ns/op | B/op | allocs/op | naive speedup |
| --- | ---: | ---: | ---: | ---: |
| string naive rebuild | 936,625 | 1,046,273 | 6,194 | 1.00x |
| string incremental | 624,009 | 1,046,272 | 6,194 | 1.50x |

The retained implementation improves CPU time by 1.50x with effectively
identical allocation behavior. The extra memory is only paid by callers that
opt into string differential maintenance; no default path changes.

## Raw Samples

```text
BenchmarkM037kDifferentialStringMinMax/naive_rebuild   1170 1021556 ns/op 1046273 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/naive_rebuild   1128 1010207 ns/op 1046273 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/naive_rebuild   1233  920069 ns/op 1046273 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/naive_rebuild   1274  936625 ns/op 1046273 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/naive_rebuild   1255  922299 ns/op 1046273 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/incremental     1917  624009 ns/op 1046272 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/incremental     1910  627432 ns/op 1046272 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/incremental     1900  622396 ns/op 1046272 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/incremental     1881  621938 ns/op 1046272 B/op 6194 allocs/op
BenchmarkM037kDifferentialStringMinMax/incremental     1888  626256 ns/op 1046272 B/op 6194 allocs/op
```
