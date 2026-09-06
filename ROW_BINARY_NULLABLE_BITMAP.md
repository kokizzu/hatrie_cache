# RowBinary Nullable Bitmap

`hatSql.EncodeSQLRowBinaryBitmap` and `DecodeSQLRowBinaryBitmap` use one
bitmap per row for nullable columns. A set bit means the corresponding
nullable value is `NULL`; non-null values use the existing schema-aware
RowBinary encoding without per-value null marker bytes.

```go
wire, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
decoded, err := hatSql.DecodeSQLRowBinaryBitmap(columns, wire)
```

The schema is still supplied out of band through `SQLRowBinaryColumn`. The
wire header contains a format marker, followed by each row's bitmap and only
the non-null column values. Non-nullable columns cannot contain `nil`.

The decoder rejects an invalid or truncated marker, truncated bitmaps or
values, invalid variable-length prefixes, nonzero unused bitmap bits, invalid
boolean bytes, unsupported types, trailing malformed rows, and row counts over
the existing RowBinary limit. Byte values are copied on decode, so decoded
mutable payloads do not alias the wire buffer.

The bitmap saves one marker byte per nullable column per row in the legacy
representation. The tradeoff is a small fixed bitmap per row and a distinct
format marker; callers should select this format when nullable-column density
makes that cheaper. It is an explicit codec and does not change the existing
RowBinary default or JSON/protobuf paths.

Focused coverage is in `hat/hatSql/row_binary_nullable_bitmap_test.go`,
including all supported nullable types, round trips, malformed data, unused
bits, non-nullable NULL rejection, wire-size reduction, and encode/decode
benchmarks against the legacy format.
