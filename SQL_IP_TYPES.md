# Typed IP Values

Hatrie Cache now has compact IPv4 and IPv6 values inspired by ClickHouse's
typed network columns. `hatSql.SQLIPv4` is a comparable four-byte value and
`hatSql.SQLIPv6` is a comparable sixteen-byte value. Both use network byte
order, so byte comparison follows address order.

```go
v4, err := hatSql.ParseSQLIPv4("192.0.2.1")
v6, err := hatSql.ParseSQLIPv6("2001:db8::1")
fmt.Println(v4.String(), v6.String())
```

SQL accepts typed literals and casts:

```sql
FROM VALUES
  (IPV4 '192.0.2.2', IPV6 '2001:db8::2'),
  (IPV4 '192.0.2.1', IPV6 '2001:db8::1')
AS values(v4, v6)
SELECT v4, v6, CAST(v4 AS TEXT), CAST(v6 AS TEXT)
WHERE v4 >= IPV4 '192.0.2.1'
ORDER BY v4
```

`hatSchema.TypeIPv4` and `hatSchema.TypeIPv6` are available for schema
validation and generated Go models. Typed JSON source declarations also accept
`IPV4` and `IPV6` fields. JSON and HTTP output uses canonical address strings;
the in-memory values remain fixed-width.

## RowBinary

`SQLRowBinaryIPv4` and `SQLRowBinaryIPv6` use fixed-width payloads of four and
sixteen bytes. Their physical IDs are appended after the existing IDs, so old
RowBinary schemas retain their meaning. The ordinary stream, streaming writer,
nullable bitmap, read statistics, and min/max statistics paths support both
types. The legacy `SQLRowBinaryString` path remains available for compatibility.

The delta and dictionary codecs intentionally keep their existing type
allowlists. Use ordinary RowBinary, stream RowBinary, or nullable-bitmap
RowBinary when an IP column is present; unsupported specialized codecs return a
validation error instead of silently changing representation.

## Tradeoffs

Typed columns reduce payload size and repeated string allocation, but address
parsing still belongs at the input boundary and canonical formatting allocates
when a string is requested. The benchmark below measures encode/decode after
values have already been parsed.

