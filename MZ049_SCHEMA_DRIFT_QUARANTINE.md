# MZ-049 Schema-Drift Quarantine

`hat/hatSql` provides an opt-in boundary for feeds whose rows may drift from
a typed schema. Valid rows continue to the accept callback; invalid rows are
sent to a quarantine callback with deterministic row, column, expected-type,
actual-type, and reason information. One bad row therefore does not have to
stop an otherwise usable source.

## API

```go
data := []byte("{\"id\":1}\n{\"id\":\"drift\"}\n")
schema, err := hatSql.InferExternalNDJSONSchema(data, hatSql.ExternalSchemaInferenceOptions{})
if err != nil {
	return err
}

stats, err := hatSql.QuarantineExternalJSONEachRow(
	bytes.NewReader(data),
	hatSql.ExternalImportOptions{},
	schema,
	hatSql.ExternalSchemaQuarantineOptions{},
	func(hatSql.Row) error { return nil },
	func(hatSql.ExternalSchemaQuarantineRecord) error { return nil },
)
_ = stats
```

The example requires the standard-library `bytes` import.

The entry points are:

- `QuarantineExternalRows` for already-decoded `[]Row` values.
- `QuarantineExternalJSONEachRow` for bounded newline-delimited JSON input.

The reader entry point uses `json.Decoder.UseNumber`, preserving large integer
values that the legacy `StreamJSONEachRow` decoder may represent as `float64`.
The legacy import and stream APIs are unchanged; callers opt in explicitly.

Both callbacks are required. Each input row is routed to exactly one callback
in input order. The function retains no rows after the callback returns. A
callback error, malformed input, or limit error stops processing and returns
the counts completed so far.

## Drift Rules

The schema is an existing `[]SQLRowBinaryColumn`, normally produced by
`InferExternalSchema` or `InferExternalNDJSONSchema`:

- Missing non-nullable columns and explicit `null` values in non-nullable
  columns are drift.
- Missing nullable columns are accepted.
- Unknown input columns are drift.
- Supported scalar types must match the declared RowBinary type.
- A `Float64` schema accepts other numeric kinds only when
  `AllowNumericPromotion` is enabled.
- JSON columns accept valid `json.RawMessage` and JSON-compatible values;
  malformed raw JSON and values that cannot be JSON encoded are drift.
- `ExternalSchemaDriftIssue.Expected` and `.Actual` identify type drift. For
  missing or unknown columns, `Reason` identifies the condition and the type
  fields may remain zero.

Issue ordering is schema order followed by lexicographically ordered unknown
columns. `MaxIssuesPerRow` bounds diagnostic memory. If more quarantine rows
would be produced than `MaxQuarantinedRows`, the function returns
`ErrExternalSchemaQuarantineLimit` instead of silently dropping the next row.

## Defaults

Zero-valued `ExternalSchemaQuarantineOptions` use bounded defaults:

| Option | Default | Maximum |
| --- | ---: | ---: |
| `MaxRows` | 4,096 | 1,000,000 |
| `MaxQuarantinedRows` | 1,024 | 1,000,000 |
| `MaxIssuesPerRow` | 64 | 1,024 |

Negative values and values above the maximum are rejected. The JSON reader
also retains the existing `ExternalImportOptions` byte and record limits; its
effective parser row limit is no larger than the quarantine `MaxRows`.

## Example Quarantine Record

For schema `id INT64 NOT NULL` and input `{"id":"wrong"}`, the quarantine
callback receives a record equivalent to:

```text
RowNumber: 7
Row:       {"id": "wrong"}
Issues:    [{Column: "id", Expected: SQLRowBinaryInt64,
             Actual: SQLRowBinaryString,
             Reason: "value type does not match schema"}]
```

The callback owns persistence, redaction, retry, and retention policy. The
library never logs or writes the quarantined payload.

## Measured Tradeoff

Benchmarks use 256 valid rows on an AMD Ryzen 9 5950X Linux/amd64 host with
`-benchmem -benchtime=200ms -count=5` through `make benchmark-mz049`.

| Workload | Median | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Existing `StreamJSONEachRow` reader | 678,334 ns | 337,770 | 7,587 | 1.00x control |
| `QuarantineExternalJSONEachRow` reader | 930,623 ns | 543,298 | 8,957 | 1.37x CPU; +60.8% B/op; +18.1% allocs |
| Existing decoded-row pass-through control | 143.6 ns | 0 | 0 | Synthetic lower-bound control |
| `QuarantineExternalRows` validation | 63,542 ns | 336 | 3 | 442.5x CPU versus lower-bound control |

The decoded-row control is intentionally minimal and is not an end-to-end
source comparison. The practical reader comparison is the first two rows.
The quarantine cost is paid only when selected, and the valid-row validator
does not allocate per row. This is an operational continuity feature, not a
hot-path optimization; callers should enable it when preserving source
progress is worth the measured validation cost.

Raw output from the after run:

```text
BenchmarkMZ049SchemaDriftBaseline-32                 1677351  143.6 ns/op       0 B/op     0 allocs/op
BenchmarkMZ049SchemaDriftBaseline-32                 1705970  141.8 ns/op       0 B/op     0 allocs/op
BenchmarkMZ049SchemaDriftBaseline-32                 1837191  147.1 ns/op       0 B/op     0 allocs/op
BenchmarkMZ049SchemaDriftBaseline-32                 1664533  148.9 ns/op       0 B/op     0 allocs/op
BenchmarkMZ049SchemaDriftBaseline-32                 1655636  133.5 ns/op       0 B/op     0 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamBaseline-32           332  663315 ns/op  337770 B/op  7587 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamBaseline-32           328  678334 ns/op  337788 B/op  7587 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamBaseline-32           322  686210 ns/op  337770 B/op  7587 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamBaseline-32           338  686581 ns/op  337770 B/op  7587 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamBaseline-32           345  674927 ns/op  337769 B/op  7587 allocs/op
BenchmarkMZ049SchemaDriftQuarantine-32                 4186   63542 ns/op     336 B/op     3 allocs/op
BenchmarkMZ049SchemaDriftQuarantine-32                 4495   58264 ns/op     336 B/op     3 allocs/op
BenchmarkMZ049SchemaDriftQuarantine-32                 3477   66090 ns/op     336 B/op     3 allocs/op
BenchmarkMZ049SchemaDriftQuarantine-32                 3754   66871 ns/op     336 B/op     3 allocs/op
BenchmarkMZ049SchemaDriftQuarantine-32                 3424   62271 ns/op     336 B/op     3 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamQuarantine-32         252  934435 ns/op  543300 B/op  8957 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamQuarantine-32         273  931492 ns/op  543297 B/op  8957 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamQuarantine-32         262  930623 ns/op  543301 B/op  8957 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamQuarantine-32         268  898659 ns/op  543298 B/op  8957 allocs/op
BenchmarkMZ049SchemaDriftJSONStreamQuarantine-32         258  883916 ns/op  543297 B/op  8957 allocs/op
```

For repeatability, run `make benchmark-mz049-before` on the clean baseline and
`make benchmark-mz049` after the implementation. Both scripts use temporary
detached worktrees and clean them on exit.
