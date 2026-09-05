# Nullable Bitmap RowBinary

`EncodeSQLRowBinaryBitmap` and `DecodeSQLRowBinaryBitmap` are an opt-in
RowBinary format for schemas with many nullable columns. Each row begins with
a bitmap containing one bit per nullable schema column; non-null values retain
the existing fixed-width and length-prefixed encodings.

```go
wire, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
if err != nil {
    return err
}
rows, err = hatSql.DecodeSQLRowBinaryBitmap(columns, wire)
```

The `HSB1` marker makes the format explicit. Existing
`EncodeSQLRowBinary`/`DecodeSQLRowBinary` bytes are unchanged for older
readers. A bitmap is still emitted when a schema has no nullable columns, so
callers should select the format based on schema shape or use the legacy
functions for narrow schemas.

## Measurement

The benchmark uses 1,024 rows and 16 nullable `int64` columns, with one in
four values NULL. Run it with:

```text
make benchmark-sql-row-binary-bitmap
```

Representative five-sample ranges on the repository benchmark host:

| Path | Time | Wire bytes | Allocations | Allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| Legacy encode | 354-367 us | 114,688 | 27 | 515,747 |
| Bitmap encode | 324-331 us | 100,356 | 11 | 396,969 |
| Legacy decode | 887-908 us | 114,688 | 13,411 | 1,374,922-1,374,931 |
| Bitmap decode | 924-938 us | 100,356 | 13,412 | 1,375,051-1,375,053 |

This workload reduces wire size by about `12%`, encode allocations by about
`59%`, and encode CPU by about `9-11%`. Decode is about `4-6%` slower because
the bitmap must be inspected, so the compact format is not made the global
default.

With 16 nullable columns that are all NULL, the per-row marker overhead falls
from 16 bytes to 2 bytes, nearly an `8x` reduction before the format marker.
