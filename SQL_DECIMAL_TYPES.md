# SQL Decimal Types

Hatrie Cache supports ClickHouse-style fixed-width decimal coefficients for
typed SQL schemas and RowBinary transfer. `SQLDecimal128` stores a signed
128-bit coefficient in 16 bytes and `SQLDecimal256` stores a signed 256-bit
coefficient in 32 bytes. The decimal scale is metadata on the owning column,
not repeated in every row.

## When To Use It

Use `DECIMAL128` for values with at most 38 decimal digits and a known fixed
scale. Use `DECIMAL256` when the value needs up to 76 digits. The existing
`DECIMAL` / `SQLDecimal` and `SQLRowBinaryString` paths remain the compatible
fallback for variable-precision or short values.

The existing `DECIMAL` type deliberately remains the default. Fixed-width
storage is not universally smaller: a short value such as `1.20` needs fewer
bytes as a string, and Decimal256 is wider than a 20-digit string. A caller
must choose the fixed type when its precision and workload justify it.

## Typed RowBinary

```go
columns := []hatSql.SQLRowBinaryColumn{{
	Name:             "amount",
	Type:             hatSql.SQLRowBinaryDecimal128,
	DecimalScale:     2,
	DecimalPrecision: 18,
}}
amount, err := hatSql.ParseSQLDecimal128("123456789012345.67", 2)
if err != nil {
	return err
}
payload, err := hatSql.EncodeSQLRowBinary(columns, []hatSql.SQLRow{
		{"amount": amount},
})
if err != nil {
	return err
}
rows, err := hatSql.DecodeSQLRowBinary(columns, payload)
if err != nil {
	return err
}
formatted, err := rows[0]["amount"].(hatSql.SQLDecimal128).Format(2)
if err != nil {
	return err
}
// formatted == "123456789012345.67"
```

`ParseSQLDecimal128` and `ParseSQLDecimal256` reject fractional digits beyond
the declared scale and reject coefficients outside the signed physical range.
They bound textual input to 128 bytes before arbitrary-precision parsing.
The optional precision metadata rejects coefficients whose absolute value is
at least `10^precision`. A precision of zero means no additional decimal
precision constraint; the physical 128-bit or 256-bit range still applies.

The wire coefficient is signed two's-complement little-endian. Nullable
columns keep the existing one-byte NULL marker. Decimal values therefore use
exactly 16 or 32 coefficient bytes when non-NULL, with no per-row length or
scale prefix.

## Self-Describing Streams

Scale cannot be inferred from a coefficient-only value. Use the explicit
column constructor for typed decimal streams:

```go
writer, err := hatSql.NewSQLRowBinaryStreamWriterWithColumns(output,
	[]hatSql.SQLRowBinaryColumn{{
		Name:             "amount",
		Type:             hatSql.SQLRowBinaryDecimal128,
		Nullable:         true,
		DecimalScale:     2,
		DecimalPrecision: 18,
	}})
if err != nil {
	return err
}
if err := writer.WriteRow(hatSql.Row{"amount": amount}); err != nil {
	return err
}
return writer.Finish()
```

The stream header carries `decimal_scale` and `decimal_precision`. The older
name-inference constructor rejects raw `SQLDecimal128` and `SQLDecimal256`
values instead of silently assuming scale zero. Legacy `SQLDecimal` strings
continue to infer as `SQLRowBinaryString`.

## Schema Types

```go
source := hatSchema.Source{
	Name: "payments",
	Columns: []hatSchema.Column{
		{Name: "amount", Type: hatSchema.TypeDecimal128,
			DecimalScale: 2, DecimalPrecision: 18, NotNull: true},
	},
}
```

Schema generation maps `TypeDecimal128` to `hatSql.SQLDecimal128` and
`TypeDecimal256` to `hatSql.SQLDecimal256`. Changing decimal scale or
precision is schema-incompatible and requires an explicit migration. Existing
legacy decimal schemas keep their fingerprints and generated `SQLDecimal`
fields.

## Persistence And Compatibility

The fixed types are supported by ordinary RowBinary, adaptive RowBinary,
nullable columns, delta batches, column statistics/min-max pruning, read-byte
accounting, and self-describing streams. Gob registration covers the exported
coefficient types for snapshot and backup payloads.

Adding a fixed decimal column type does not rewrite existing data. Existing
backups and streams using `DECIMAL` remain on the legacy representation. A
new fixed-width schema must be restored with the matching scale and precision
metadata.

## Measured Tradeoffs

The 10,000-row benchmark uses a 20-digit integer part and four fractional
digits. Values are parsed before timing for the typed cases. Medians are from
five samples on the repository benchmark host; see `BENCHMARK.md` for raw
samples and the command used.

| Path | String control | Decimal128 | Decimal256 |
| --- | ---: | ---: | ---: |
| Encode time | 375,037 ns/op | 428,953 ns/op (1.14x slower) | 611,345 ns/op (1.63x slower) |
| Decode time | 1,042,821 ns/op | 1,026,659 ns/op (1.02x faster) | 1,020,983 ns/op (1.02x faster) |
| Payload | 260,000 bytes | 160,000 bytes (1.63x smaller) | 320,000 bytes (1.23x larger) |
| Encode heap | 1,186,537 B/op | 686,832 B/op (1.73x lower) | 1,538,784 B/op (1.30x higher) |
| Decode heap | 4,228,764 B/op | 3,908,759 B/op (1.08x lower) | 4,068,756 B/op (1.04x lower) |
| Encode allocations | 26 | 24 (1.08x lower) | 26 (same) |
| Decode allocations | 40,076 | 30,076 (1.33x lower) | 30,076 (1.33x lower) |

The result supports Decimal128 as a compact typed transfer choice when a
small encode CPU increase is acceptable. Decimal256 is a precision choice,
not a general compression choice. Do not switch a short or latency-critical
legacy decimal workload without measuring its own value lengths and CPU mix.
