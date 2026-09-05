# RowBinary Column Statistics

`hat/hatSql` provides an opt-in RowBinary envelope with exact per-column
statistics:

```go
wire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, rows)
if err != nil {
	return err
}

rows, stats, err := hatSql.DecodeSQLRowBinaryWithStats(columns, wire)
if err != nil {
	return err
}
```

`BuildSQLRowBinaryColumnStats` can compute the metadata without making an
envelope. Each `SQLRowBinaryColumnStats` reports the schema column name,
`NullCount`, `ValueCount`, and, for orderable physical types, an observed
non-NULL `Min` and `Max`. JSON columns report counts only. Float NaN values are
counted as present but excluded from min/max; infinities remain valid extrema.
Byte and UUID ordering is unsigned lexicographic, strings use Go byte ordering,
and dates are normalized to UTC midnight in the same way as RowBinary values.

## Envelope Format

The stream begins with the four-byte ASCII magic `HBS1`, followed by:

1. A uvarint row count.
2. A uvarint metadata length and metadata for every schema column in order.
3. A uvarint RowBinary payload length and the ordinary RowBinary payload.

Each column metadata record contains a uvarint NULL count, a uvarint value
count, and a one-byte min/max marker. When the marker is set, the typed min and
max use the existing RowBinary physical encoding. Column names and types are
not repeated, so the receiver must provide the same ordered schema.

The decoder validates framing, count totals, typed min/max ordering, the
RowBinary payload, and then recomputes the metadata from decoded rows. Stale or
tampered statistics are rejected rather than being trusted for pruning. Empty
payloads are valid and return zero counts. Existing JSON, protobuf, and plain
RowBinary defaults are unchanged.

The envelope bounds metadata at 64 MiB and inherits the existing one-million
row RowBinary limit. Malformed lengths, invalid markers, unsupported min/max
types, truncated values, payload errors, and trailing bytes are rejected.

## Measured Tradeoff

These five-run medians used `make benchmark-row-binary-stats` on an AMD Ryzen 9
5950X with 256 rows, one numeric column, and three repeated string-like
columns. The baseline uses plain RowBinary on the same rows.

| Operation | Stats envelope | Plain RowBinary | Relative result |
| --- | ---: | ---: | --- |
| Encode | 66,825 ns/op; 86,078 B; 538 allocs; 11,345 B wire | 27,976 ns/op; 46,586 B; 16 allocs; 11,264 B wire | 2.39x slower; 1.85x higher heap; 33.63x more allocs; 1.01x larger wire |
| Decode and verify | 116,736 ns/op; 122,473 B; 2,325 allocs | 76,416 ns/op; 111,490 B; 1,801 allocs | 1.53x slower; 1.10x higher heap; 1.29x more allocs |

Use this path when exact block-level counts and min/max metadata can skip work
or support diagnostics. Keep plain RowBinary for general transfer when the
statistics will not be consumed.

Run the focused tests and benchmark with:

```sh
make test-row-binary-stats
make benchmark-row-binary-stats
```
