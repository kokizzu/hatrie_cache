# RowBinary Delta Codecs

`hatSql` keeps `EncodeSQLRowBinary` and `DecodeSQLRowBinary` unchanged for
backward compatibility. New transfers that have monotonically changing
integer or time columns can opt into:

- `EncodeSQLRowBinaryDelta`: first-order signed delta varints (`HSD1`).
- `EncodeSQLRowBinaryDoubleDelta`: second-order delta varints (`HSD2`) for
  regularly advancing counters and timestamps.
- `DecodeSQLRowBinaryDelta`: decodes either `HSD1` or `HSD2`.

The schema remains out of band and must have the same ordered columns on both
sides. Nullable markers and all existing RowBinary types remain supported.
Integer, date, datetime, and duration columns use delta values. Strings,
bytes, JSON, booleans, floats, and UUIDs retain their existing representation.
Missing or malformed input is rejected before a successful result is returned;
the decoder also enforces the existing one-million-row limit.

## Example

```go
wire, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
if err != nil {
    return err
}
rows, err = hatSql.DecodeSQLRowBinaryDelta(columns, wire)
```

Use the legacy functions when communicating with an older reader. The delta
functions are additive APIs and do not change existing journal, backup, or
HTTP behavior.

## Measurement

The benchmark uses 1,024 rows with an `int64` id, `uint64` sequence,
`DateTime`, and repeated string. It was run with:

```text
make benchmark-sql-row-binary-delta
```

Representative output on the repository benchmark host, with five samples per
case:

| Path | Time | Wire bytes | Allocations | Allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| Legacy encode | 105-113 us | 31,744 | 20 | 154,360 |
| Delta encode | 94-109 us | 14,348 | 6 | 54,528 |
| Double-delta encode | 86-95 us | 10,266 | 5 | 38,144 |
| Legacy decode | 283-304 us | 31,744 | 6,925 | 437,378 |
| Delta decode | 309-326 us | 14,348 | 6,914 | 417,034 |
| Double-delta decode | 295-310 us | 10,266 | 6,914 | 417,034 |

For this workload, first-order delta is about `2.2x` smaller and double-delta
about `3.1x` smaller than legacy. Encode CPU and allocations improve, while
first-order decode is about `1.05x` slower and double-delta is approximately
neutral. Random or highly changing values can reduce or erase the size gain,
so no global default is changed without a format negotiation or adaptive
selection policy.
