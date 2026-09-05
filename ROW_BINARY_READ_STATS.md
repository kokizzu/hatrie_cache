# RowBinary Read Statistics

`AnalyzeSQLRowBinaryRead` validates a RowBinary stream and reports the bytes,
non-null values, and NULL values consumed by each schema column without
materializing rows or variable-length values.

```go
stats, err := hatSql.AnalyzeSQLRowBinaryRead(columns, wire)
if err != nil {
    return err
}
for _, column := range stats.Columns {
    log.Printf("%s: %d bytes, %d values, %d nulls",
        column.Name, column.Bytes, column.Values, column.Nulls)
}
```

`Bytes` includes nullable markers. For valid input, the sum of all column
bytes equals `stats.Bytes`, and `stats.Rows` is the number of complete rows.
Malformed input and oversized streams are rejected. Existing decode APIs and
wire formats are unchanged.

## Measurement

The benchmark uses 1,024 rows with an `int64`, nullable string, and datetime.
Run it with:

```text
make benchmark-sql-row-binary-read-stats
```

Representative five-sample ranges on the repository benchmark host:

| Path | Time | Allocated bytes | Allocations |
| --- | ---: | ---: | ---: |
| Statistics-only analysis | 17.2-17.3 us | 128 | 1 |
| Full legacy decode | 251-258 us | 429,178-429,179 | 5,900 |

The statistics-only path is approximately `14-15x` faster and uses roughly
`3,300x` less allocated memory for this diagnostic workload. It is intended
for observability and admission decisions, not as a replacement for decoding
rows that the caller needs.
