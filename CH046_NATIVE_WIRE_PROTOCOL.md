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
  encoding byte (version 2 only: raw, Flate, or dictionary)
  column payload length
  column payload
end frame (0)
```

Column payload values reuse the existing SQL RowBinary physical encodings and
NULL markers. The reader enforces sequential block indexes, cumulative
progress, schema column count, a 65,536-row block limit, and a 64 MiB total
payload limit per block. Truncated input, invalid markers, trailing column
bytes, unknown frames, and inconsistent progress are rejected.

The default block size is 1,024 rows. The format is deliberately opt-in: a
full decode/encode path has a small framing and buffering cost, while a
projected decode can skip wide unrequested columns.

### Dictionary Encoding

`SQLColumnarBlockStreamCompressionDictionary` is an explicit version-2 mode
for repeated `String` columns. Each eligible column stores a bounded dictionary
of unique strings followed by one dictionary ID per row; nullable columns keep
their existing NULL markers. A dictionary is emitted only when it is smaller
than the raw column payload. Integer, bytes, date, JSON, and other physical
types remain raw.

When a dictionary column is materialized, the reader bounds its entries by the
block row count and the existing 64 MiB decoded-block limit. Invalid entry
lengths, IDs, NULL markers, truncated payloads, and trailing bytes are
rejected. Projected-away columns remain length-bounded without materializing
their values. The zero-value writer options still emit the legacy raw v1
format, and dictionary mode is never selected implicitly.

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
Dictionary mode can reduce repeated-string wire bytes substantially, but its
CPU and allocation cost makes it a deliberate bandwidth-for-CPU tradeoff; see
`CH046_WIRE_DICTIONARY.md` for the measured boundary.
