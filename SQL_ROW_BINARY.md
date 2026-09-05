# SQL RowBinary

`hat/hatSql` provides `EncodeSQLRowBinary` and `DecodeSQLRowBinary` for
schema-aware row transfer. The format is an additive, RowBinary-style stream:
the ordered schema is supplied out of band, fixed-width values are little
endian, and strings, bytes, and JSON use unsigned-varint length prefixes.

It is intended for internal or service-to-service transfer where both sides
already have the same schema. It is not a change to the default JSON,
protobuf, HTTP, or gRPC formats, and it is not a drop-in ClickHouse server
protocol negotiation feature.

## Supported Types

| Type | Go value | Wire representation |
| --- | --- | --- |
| `SQLRowBinaryInt64` | signed integer types | 8-byte little-endian integer |
| `SQLRowBinaryUint64` | unsigned integer types | 8-byte little-endian integer |
| `SQLRowBinaryFloat64` | `float32`, `float64` | 8-byte IEEE-754 value |
| `SQLRowBinaryBool` | `bool` | one byte, `0` or `1` |
| `SQLRowBinaryString` | `string` | varint byte length plus UTF-8 bytes |
| `SQLRowBinaryBytes` | `[]byte` | varint byte length plus bytes |
| `SQLRowBinaryDate` | `time.Time` | signed 32-bit UTC day number |
| `SQLRowBinaryDateTime` | `time.Time` | signed Unix nanoseconds |
| `SQLRowBinaryDuration` | `time.Duration`, `int64` | signed nanoseconds |
| `SQLRowBinaryUUID` | `[16]byte` | 16 raw bytes |
| `SQLRowBinaryJSON` | `json.RawMessage` | varint byte length plus JSON bytes |

Set `Nullable: true` on a column to add a one-byte marker before its value:
`0` means present and `1` means NULL. Missing map fields are treated as NULL.
Non-nullable missing or nil fields, invalid types, duplicate column names, and
malformed input return errors.

## Example

```go
columns := []hatSql.SQLRowBinaryColumn{
    {Name: "id", Type: hatSql.SQLRowBinaryInt64},
    {Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
}
rows := []hatSql.SQLRow{{"id": int64(7), "name": "alpha"}}

payload, err := hatSql.EncodeSQLRowBinary(columns, rows)
decoded, err := hatSql.DecodeSQLRowBinary(columns, payload)
```

The decoder reads rows until the payload is exhausted and rejects partial rows
and invalid nullable markers. Both directions enforce a bounded maximum of one
million rows.
