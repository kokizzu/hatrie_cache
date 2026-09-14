# TR-018 Zero-Copy RowBinary Reader

Status: implemented and opt-in.

This is a Tarantool-inspired tuple-field optimization for RowBinary consumers
that can process bytes without materializing a `SQLRow` map. It is useful for
filters, projections, and forwarding paths that do not need typed Go values.

## API

```go
reader, err := hatSql.NewSQLRowBinaryBorrowedReader(columns, encoded)
if err != nil {
	return err
}
for reader.Next() {
	row := reader.Row()
	name := row.Fields[1].Data
	// Consume name before the next call to Next.
	_ = name
}
if err := reader.Err(); err != nil {
	return err
}
```

`SQLRowBinaryBorrowedField.Data` is the raw field payload backed by
`encoded`. Variable-length fields exclude their RowBinary length prefix. Fixed
fields retain their wire representation and width, so callers that need a
typed value can decode it without an intermediate copy. `Null` distinguishes a
NULL field from a non-NULL empty payload.

`Reset` reuses the reader and its field slice for another payload. The schema
is validated when the reader is constructed. Payload validation remains
incremental in `Next`, including bounds checks, nullable markers, enum and
decimal metadata, date/bool checks, and JSON validity.

## Ownership Rules

- `Data` is valid only until the next `Next` or `Reset` call.
- Keep `encoded` alive and unchanged while borrowed fields are in use.
- Copy a field explicitly with `append([]byte(nil), field.Data...)` when it
  must outlive the current row.
- Do not pass borrowed fields to another goroutine unless the caller provides
  synchronization and preserves the input lifetime.
- The reader uses no `unsafe` conversion and does not mutate the input.

Use `DecodeSQLRowBinary` when the caller needs owned `string`, `[]byte`, JSON,
IP, UUID, decimal, and time values or a row map. The existing decoder remains
the default and its wire format is unchanged.

## Measurement

Command: `make benchmark-tr018-zero-copy-row-binary`. Five samples were run
for the same 512-row, five-column payload on Linux/amd64, AMD Ryzen 9 5950X.
Both paths process the same encoded bytes; the borrowed benchmark consumes
each returned field length and NULL marker in a checksum.

| Path | Raw `ns/op` samples | Median | Throughput median | Heap | Allocs | Relative |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Existing copied decoder | 206590; 194326; 206074; 198648; 202964 | 202964 | 214.85 MB/s | 243129 B/op | 3998 | 1.00x |
| Borrowed reader | 67649; 64886; 68990; 76339; 70687 | 68990 | 632.06 MB/s | 0 B/op | 0 | 2.94x faster |

The encoded payload size and bandwidth on the wire do not change. The win is
on the receiving process: no per-row maps, typed values, or backing-buffer
copies are allocated. The tradeoff is that consumers must understand the
schema and wire representation, explicitly copy data they retain, and accept
that a long-lived borrowed field keeps the complete encoded payload reachable.
