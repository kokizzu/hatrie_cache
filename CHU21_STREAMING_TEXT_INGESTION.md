# CH-U21 Streaming Text Ingestion

`ExternalTables` now accepts bounded readers for CSV and JSONEachRow input.
The parser consumes one record at a time, and callback APIs let callers feed
their own storage without building an intermediate table snapshot.

## APIs

```go
options := hatSql.ExternalImportOptions{
	MaxRows:        1_000_000,
	MaxBytes:       1 << 30,
	MaxRecordBytes: 16 << 20,
}

tables := hatSql.NewExternalTables()
err := tables.ImportCSVReader("events", reader, options)
err = tables.ImportJSONEachRowReader("events", reader, options)

err = hatSql.StreamCSV(reader, options, func(columns, record []string) error {
	return consumeCSV(columns, record)
})
err = hatSql.StreamJSONEachRow(reader, options, func(row hatSql.Row) error {
	return consumeJSON(row)
})
```

`ImportNDJSONReader` and `StreamNDJSON` are aliases for the JSONEachRow APIs.
CSV values remain strings, while JSON values retain their decoded JSON types.
Callback record slices may be reused after the callback returns; copy them if
they must be retained.

## Safety contract

Reader options default to one million data rows, one GiB of consumed input,
and 16 MiB per JSONEachRow line. Negative values and an excessively large byte
limit are rejected. A reader import replaces the existing table only after
the complete input has parsed and passed its limits, so malformed input,
callback errors, and limit errors leave the previous snapshot unchanged.

The older `ImportCSV`, `ImportJSON`, and `ImportNDJSON` byte-slice methods keep
their existing behavior. Reader APIs do not open paths or make network
requests; callers own the reader and its lifecycle.

## Measured result

The 20,000-row benchmark uses 200 ms samples on an AMD Ryzen 9 5950X Linux
`amd64` host. Reader imports use owned-snapshot registration, avoiding the
second internal row clone that the legacy path retains for compatibility.

| Workload | Legacy whole-buffer | Reader import | Relative result |
| --- | ---: | ---: | --- |
| CSV | 14.96 ms, 17.65 MB, 160,036 allocs | 8.73 ms, 8.45 MB, 100,038 allocs | 1.71x faster, 2.09x lower transient bytes, 1.60x fewer allocs |
| JSONEachRow | 32.85 ms, 19.53 MB, 319,994 allocs | 28.61 ms, 12.76 MB, 280,016 allocs | 1.15x faster, 1.53x lower transient bytes, 1.14x fewer allocs |

Callback-only parsing avoids table registration and the final table clone. Its
median was 1.71 ms and 0.32 MB for CSV, and 20.70 ms and 12.00 MB for
JSONEachRow. The JSON path still allocates one decoded row map per object; the
reader API removes whole-input splitting and snapshot duplication, not JSON
decoding allocations. Full raw samples are in [BENCHMARK.md](BENCHMARK.md#ch-u21-streaming-text-ingestion).
