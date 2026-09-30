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
