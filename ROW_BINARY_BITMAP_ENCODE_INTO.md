# Reusable Nullable-Bitmap RowBinary Encoding

`EncodeSQLRowBinaryBitmap` is the explicit HSB1 transfer format for rows with
nullable columns. It writes one NULL bitmap per row and keeps the schema out of
band. For repeated same-shape batches, `EncodeSQLRowBinaryBitmapInto` reuses a
caller-owned destination:

```go
wire, err := hatSql.EncodeSQLRowBinaryBitmapInto(nil, columns, rows)
if err != nil {
	return err
}
for _, nextBatch := range batches {
	wire, err = hatSql.EncodeSQLRowBinaryBitmapInto(wire[:0], columns, nextBatch)
	if err != nil {
		return err
	}
	send(wire)
}
```

The returned bytes may alias the destination and are invalidated when that
buffer is reused. The HSB1 bytes, NULL validation, row limits, and decode
behavior are unchanged. The existing allocating function remains available and
delegates to the same validated implementation. This API is explicit and
opt-in; adaptive HSA1 selection does not pay bitmap encoding cost.

## Measurement

The 4,096-row workload used nullable `INT64`, string, bytes, and boolean
columns and emitted 124,342 wire bytes. Ten samples on an AMD Ryzen 9 5950X
produced these medians:

| Operation | ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `EncodeSQLRowBinaryBitmap` | 384,008 | 451,075 | 18 | 1.00x |
| Warm `EncodeSQLRowBinaryBitmapInto` | 277,549 | 559 | 8 | 1.38x faster |

The output is byte-for-byte identical, so the improvement is allocation and
CPU reuse rather than a bandwidth tradeoff.

## Reusable nullable-bitmap decoding

`DecodeSQLRowBinaryBitmapInto` reuses row maps and string/bytes/JSON value
buffers when the caller passes the previous result as `dst[:0]`:

```go
decoded, err := hatSql.DecodeSQLRowBinaryBitmapInto(nil, columns, wire)
if err != nil {
	return err
}
for _, nextWire := range laterWires {
	decoded, err = hatSql.DecodeSQLRowBinaryBitmapInto(decoded[:0], columns, nextWire)
	if err != nil {
		return err
	}
	consume(decoded)
}
```

The allocating `DecodeSQLRowBinaryBitmap` API remains available and delegates
to the same validated implementation. HSB1 framing, malformed-input errors,
NULL semantics, and value ownership are unchanged; the reusable decoder is
single-owner and the caller owns the destination slice.

On the same ten-sample 4,096-row workload, warm `DecodeInto` measured 745,210
ns/op, 163,151 B/op, and 10,481 allocations versus 1,321,556 ns/op,
1,794,134 B/op, and 25,038 allocations before reuse: 1.77x faster, 11.0x
less allocated heap, and 2.39x fewer allocations.
