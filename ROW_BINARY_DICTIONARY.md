# Stateful RowBinary Dictionary Batches

`hat/hatSql` provides an opt-in stateful variant of RowBinary for repeated
string-like values across compatible batches:

```go
columns := []hatSql.SQLRowBinaryColumn{
	{Name: "id", Type: hatSql.SQLRowBinaryInt64},
	{Name: "region", Type: hatSql.SQLRowBinaryString},
	{Name: "payload", Type: hatSql.SQLRowBinaryBytes},
}
dictionaryColumns := []string{"region", "payload"}

encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, dictionaryColumns)
if err != nil {
	return err
}
decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, dictionaryColumns)
if err != nil {
	return err
}

wire, err := encoder.Encode(rows)
if err != nil {
	return err
}
decoded, err := decoder.Decode(wire)
if err != nil {
	return err
}
```

The encoder and decoder retain state between calls. The first batch sends new
dictionary values; later batches send compact ids and only append values that
were not seen before. Both sides must use the same ordered schema and selected
dictionary columns. Selection is explicit so callers can leave high-cardinality
or mostly unique columns on the ordinary RowBinary path.

`SQLRowBinaryString`, `SQLRowBinaryBytes`, and `SQLRowBinaryJSON` can be
dictionary encoded. Values are matched byte-for-byte; JSON is not normalized.
Non-selected columns use the existing RowBinary value representation, including
nullable markers.

## Batch Format

Each batch starts with the four-byte ASCII magic `HDB1`, followed by:

1. A uvarint row count.
2. For every selected dictionary column in schema order, a uvarint count of
   new values followed by each value as a uvarint length and raw bytes.
3. Row values in schema order. A dictionary value is a uvarint id; other values
   retain the ordinary RowBinary encoding.

The format is stateful: a batch that refers to prior dictionary ids cannot be
decoded by a fresh decoder. `Reset` clears retained dictionaries on either
side while retaining reusable capacity. A failed encode or decode does not
commit partial dictionary state.

The implementation bounds each batch at 1,000,000 rows, each dictionary at
1,000,000 entries, each value at 64 MiB, and each dictionary's retained bytes
at 1 GiB. Malformed headers, lengths, ids, null markers, truncated values, and
trailing bytes are rejected.

## Measured Tradeoff

These five-run medians used `make benchmark-row-binary-dictionary` on an AMD
Ryzen 9 5950X with 256 rows and one numeric plus three repeated string-like
columns. The plain baseline uses `EncodeSQLRowBinary` and
`DecodeSQLRowBinary` on the same rows.

| Operation | Dictionary path | Plain RowBinary | Relative result |
| --- | ---: | ---: | --- |
| First encode | 55,977 ns/op; 17,556 B; 64 allocs; 3,162 B wire | 25,658 ns/op; 34,298 B; 15 allocs; 8,512 B wire | 2.18x slower; 1.95x lower heap; 4.27x more allocs; 2.69x smaller wire |
| Reused encode | 37,211 ns/op; 15,931 B; 14 allocs; 3,081 B wire | 25,658 ns/op; 34,298 B; 15 allocs; 8,512 B wire | 1.45x slower; 2.15x lower heap; 1.07x fewer allocs; 2.76x smaller wire |
| First decode | 79,611 ns/op; 111,209 B; 1,808 allocs | 76,933 ns/op; 110,977 B; 1,801 allocs | 1.03x slower; effectively neutral heap; 1.00x more allocs |
| Reused decode | 74,135 ns/op; 110,961 B; 1,794 allocs | 76,933 ns/op; 110,977 B; 1,801 allocs | 1.04x faster; effectively neutral heap; 1.00x fewer allocs |

Use it when bandwidth is more constrained than encode CPU and the selected
values repeat. Plain RowBinary remains the compatibility and general-purpose
default.

Run the focused tests and benchmark with:

```sh
make test-row-binary-dictionary
make benchmark-row-binary-dictionary
```
