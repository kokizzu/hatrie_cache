# CH-046 Native Wire Protocol

Hatrie now has an opt-in typed columnar block stream for SQL query results.
It is intended for clients that project a small number of fields from wide
results, where decoding every RowBinary field is unnecessary.

## Selection

Existing defaults are unchanged. Streaming SQL continues to use NDJSON unless
the client explicitly requests another format. Request the columnar stream with
the `Accept` header:

```text
Accept: application/x-hatrie-columnar
```

The HTTP endpoint returns `Content-Type: application/x-hatrie-columnar`. A
quality value of `q=0` disables the format and falls through to the next
accepted format.

## Go API

The codec is importable from `hatrie_cache/hat/hatSql`:

```go
writer := hatSql.NewSQLColumnarBlockStreamWriterWithColumns(w, columns, 1024)
for _, row := range rows {
	if err := writer.WriteRow(row); err != nil {
		return err
	}
}
if err := writer.Finish(); err != nil {
	return err
}
```

For repeated string columns, dictionary encoding is available explicitly:

```go
writer := hatSql.NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
	w,
	columns,
	1024,
	hatSql.SQLColumnarBlockStreamOptions{
		Dictionary: hatSql.SQLColumnarBlockStreamDictionaryAuto,
	},
)
```

`DictionaryAuto` is applied per block and only to `SQLRowBinaryString`
columns. It keeps the dictionary payload only when it is smaller than the raw
column payload; high-cardinality columns fall back to raw v2 payloads. A zero
options value remains the raw v1 format.

Readers can decode every column or project only the requested fields:

```go
reader, err := hatSql.NewSQLColumnarBlockStreamReader(r)
if err != nil {
	return err
}
for reader.NextBlockFields([]string{"id", "state"}) {
	consume(reader.Block())
}
if err := reader.Err(); err != nil {
	return err
}
```

`NextBlockFields` still consumes skipped payloads and validates their framing,
but does not decode or allocate their values. `Progress()` reports only blocks
that have been completely emitted or consumed.

## Wire Layout

The stream starts with the four-byte magic `HCB1`, a uvarint header length,
and a JSON header containing the version, schema, and block-row limit. Frames
follow until an explicit end frame:

```text
data frame (1)
block index
rows in block
cumulative blocks
cumulative rows
column count
repeat for each schema column:
  v2 encoding byte (raw, Flate, or dictionary)
  column payload length
  column payload
end frame (0)
```

Version 1 omits the encoding byte and always carries raw SQL RowBinary column
payloads. Version 2 uses encoding `0` for raw, `1` for bounded Flate, and `2`
for a string dictionary. Dictionary payloads contain a bounded entry count,
length-prefixed strings, then one uvarint value ID per non-NULL row; nullable
columns retain their existing one-byte NULL markers. The reader rejects entry
counts larger than the block row count, IDs outside the dictionary, invalid
markers, trailing bytes, and logical decoded data over the 64 MiB block limit.
The reader enforces sequential block indexes, cumulative progress, schema
column count, and a 65,536-row block limit. Truncated input, unknown frames,
and inconsistent progress are rejected.

The default block size is 1,024 rows. The format is deliberately opt-in: a
full decode/encode path has a small framing and buffering cost, while a
projected decode can skip wide unrequested columns.

## Benchmark

Command:

```text
make benchmark-ch046-native-wire-protocol
```

Fixture: 8,192 rows, four typed columns (`int64`, repeated string,
`float64`, and `bool`), five benchmark samples on an AMD Ryzen 9 5950X.
The median values below are representative; benchmark variance depends on
system load.

| Path | Median ns/op | B/op | allocs/op | Wire bytes | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Existing RowBinary encode | 833,587 | 908,030 | 26 | 186,368 | baseline |
| Columnar block encode | 947,050 | 743,374 | 152 | 186,753 | 1.14x slower CPU, 0.82x B/op |
| JSON encode | 7,331,996 | 2,861,515 | 73,732 | 491,862 | 8.80x slower CPU, 2.64x wire |

Projection fixture: the same rows plus a 256-byte payload column; only the
`category` column is requested during decode.

| Path | Median ns/op | B/op | allocs/op | Wire bytes | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Existing RowBinary decode all fields | 2,578,457 | 5,744,372 | 65,350 | 2,299,904 | baseline |
| Columnar decode with `category` projection | 1,913,049 | 3,068,899 | 32,904 | 2,300,358 | 1.35x faster, 0.53x B/op, 0.50x allocs |

The columnar stream is therefore not a universal replacement for RowBinary.
Its measured win is selective decode of wide results. Full-stream encoding is
slower and has more allocations, so the server does not make it the default.

Dictionary benchmark command:

```text
make benchmark-ch046-wire-dictionary
```

Fixture: 4,096 rows, repeated `state` and `region` strings, 256 rows per
block, five samples per mode on the same AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op | Wire bytes | Tradeoff vs raw |
| --- | ---: | ---: | ---: | ---: | --- |
| Raw v1 encode | 471,239 | 351,661 | 196 | 119,216 | baseline |
| Dictionary auto encode | 977,239 | 571,709 | 564 | 46,176 | 2.07x CPU, 1.63x heap, 2.88x allocs, 2.58x smaller wire |
| Raw v1 decode | 1,780,874 | 1,804,492 | 28,607 | 119,216 | baseline |
| Dictionary decode | 2,681,900 | 1,765,619 | 28,669 | 46,176 | 1.51x CPU, 2.2% lower heap, 0.22% more allocs |

The high-cardinality control falls back to raw v2: median encode is 411,931
ns/op, 482,753 B/op, and 2,336 allocs/op versus raw v1 at 212,559 ns/op,
216,887 B/op, and 143 allocs/op. Its wire output is only 16 bytes larger from
v2 framing. This is why dictionary encoding is explicit and why admission is
bounded to low-cardinality blocks rather than being the default.
