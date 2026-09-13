# Compact SQL Enum Types

`hatSql` supports schema-aware `ENUM8` and `ENUM16` physical values for
RowBinary transfer. Enum codes are zero-based indexes into the ordered
`SQLRowBinaryColumn.EnumValues` list. The labels remain schema metadata; the
row payload contains only the code.

## RowBinary

```go
columns := []hatSql.SQLRowBinaryColumn{
	{
		Name:       "status",
		Type:       hatSql.SQLRowBinaryEnum8,
		EnumValues: []string{"queued", "running", "done"},
	},
}
rows := []hatSql.SQLRow{
	{"status": "queued"},
	{"status": hatSql.SQLEnum8(2)},
}

encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
// encoded is []byte{0, 2}.
decoded, err := hatSql.DecodeSQLRowBinary(columns, encoded)
// decoded is []hatSql.SQLRow{{"status": hatSql.SQLEnum8(0)},
// 	{"status": hatSql.SQLEnum8(2)}}.
```

`ENUM8` uses one byte per non-NULL code and supports up to 256 labels.
`ENUM16` uses two little-endian bytes and supports up to 65,536 labels. Encode
accepts a label, the matching typed code, or a bounded integer code. Decode
returns `SQLEnum8` or `SQLEnum16` so it does not allocate a label string for
each row. Use `SQLEnum8.Label(labels)` or `SQLEnum16.Label(labels)` when a
display label is needed.

The same enum values work with nullable RowBinary bitmaps, adaptive/delta
RowBinary, column statistics, read accounting, and parallel decoding. Stream
headers carry optional `enum_values` metadata. The streaming writer can emit
typed enum codes without labels because its schema is physical and inferred;
applications that need label resolution must keep the label schema out of
band or provide it in the stream header.

Unknown labels, negative codes, codes outside the label list, duplicate labels,
empty labels, and labels attached to a non-enum type are rejected. Invalid
codes are also rejected while decoding a strict schema, including stats
metadata. Enum label changes make rolling schema compatibility fail; publish a
new compatible schema only after all readers understand the new label order.

## SQL Schema Models

```go
schema := hatSchema.Schema{Sources: map[string]hatSchema.Source{
	"jobs": {
		Name: "jobs",
		Columns: []hatSchema.Column{
			{
				Name:       "status",
				Type:       hatSchema.TypeEnum8,
				EnumValues: []string{"queued", "running", "done"},
			},
		},
	},
}}
```

`hatSchema.GenerateGoModels` maps `TypeEnum8` to `hatSql.SQLEnum8` and
`TypeEnum16` to `hatSql.SQLEnum16`. Enum labels are included in schema
fingerprints and are deep-copied by `Schema.Clone`.

## Benchmark

The fixture uses 10,000 rows cycling through four labels on the same machine
(`AMD Ryzen 9 5950X`, Linux, Go benchmark `-benchmem`). The string control
uses the existing `SQLRowBinaryString` path; the enum path uses typed
`SQLEnum8` codes.

| Operation | String control | Typed enum | Improvement |
| --- | ---: | ---: | ---: |
| Encode median | 252,013 ns/op; 285,432 B/op; 22 allocs/op | 210,750 ns/op; 46,584 B/op; 16 allocs/op | 1.20x CPU; 6.13x lower B/op; 1.38x fewer allocs |
| Default decode median | 1,049,397 ns/op; 3,988,640 B/op; 40,077 allocs/op | 945,255 ns/op; 3,748,750 B/op; 20,076 allocs/op | 1.11x CPU; 1.06x lower B/op; 2.00x fewer allocs |
| Wire payload | 67,500 bytes | 10,000 bytes | 6.75x smaller |
| Serial decode median | 1,855,304 ns/op; 3,910,392 B/op; 40,018 allocs/op | 1,607,696 ns/op; 3,670,395 B/op; 20,018 allocs/op | 1.15x CPU; 1.07x lower B/op; 2.00x fewer allocs |
| Forced parallel decode median | 1,057,517 ns/op; 3,988,640 B/op; 40,077 allocs/op | 920,112 ns/op; 3,748,748 B/op; 20,076 allocs/op | 1.15x CPU; 1.06x lower B/op; 2.00x fewer allocs |

The default decoder uses the compact fixed-width parallel fast path for enum
payloads with at least 256 rows. Smaller payloads stay serial to avoid
scheduling overhead. The benchmark therefore reports both default behavior
and equal-decoder comparisons.

### Raw Baseline Before Enum Implementation

```text
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 4914 217659 ns/op 67500 payload-bytes 285433 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 5214 216533 ns/op 67500 payload-bytes 285433 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 4854 220117 ns/op 67500 payload-bytes 285432 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 5349 218849 ns/op 67500 payload-bytes 285432 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 5331 219477 ns/op 67500 payload-bytes 285432 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1130 1000076 ns/op 67500 payload-bytes 3988784 B/op 40077 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1192 996951 ns/op 67500 payload-bytes 3988629 B/op 40076 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1206 1007535 ns/op 67500 payload-bytes 3988625 B/op 40076 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1276 997390 ns/op 67500 payload-bytes 3988642 B/op 40076 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1177 993909 ns/op 67500 payload-bytes 3988606 B/op 40076 allocs/op
```

### Raw Final Comparison

```text
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 4287 254657 ns/op 67500 payload-bytes 285435 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 4418 247417 ns/op 67500 payload-bytes 285432 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineEncode-32 4506 252013 ns/op 67500 payload-bytes 285432 B/op 22 allocs/op
BenchmarkSQLRowBinaryEnumTypedEncode-32 5713 208674 ns/op 10000 payload-bytes 46584 B/op 16 allocs/op
BenchmarkSQLRowBinaryEnumTypedEncode-32 5480 210750 ns/op 10000 payload-bytes 46584 B/op 16 allocs/op
BenchmarkSQLRowBinaryEnumTypedEncode-32 5067 222568 ns/op 10000 payload-bytes 46584 B/op 16 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1117 1039284 ns/op 67500 payload-bytes 3988831 B/op 40077 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1156 1055157 ns/op 67500 payload-bytes 3988640 B/op 40077 allocs/op
BenchmarkSQLRowBinaryEnumStringBaselineDecode-32 1183 1049397 ns/op 67500 payload-bytes 3988631 B/op 40076 allocs/op
BenchmarkSQLRowBinaryEnumTypedDecode-32 1342 950610 ns/op 10000 payload-bytes 3748757 B/op 20076 allocs/op
BenchmarkSQLRowBinaryEnumTypedDecode-32 1254 945255 ns/op 10000 payload-bytes 3748750 B/op 20076 allocs/op
BenchmarkSQLRowBinaryEnumTypedDecode-32 1245 916937 ns/op 10000 payload-bytes 3748740 B/op 20076 allocs/op
```

The full reproducible commands are `make benchmark-sql-enum-before`,
`make benchmark-sql-enum-encode`, `make benchmark-sql-enum-default`, and
`make benchmark-sql-enum-comparison`.
