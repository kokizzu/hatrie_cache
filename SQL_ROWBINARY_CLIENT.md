# SQL RowBinary Client Streams

`hatSql.Conn` can consume the SQL endpoint's compact RowBinary stream without
materializing the complete result. This is useful for large result sets where
wire bandwidth and per-row JSON decoding cost matter.

## Pull-based API

```go
reader, err := conn.QueryRowBinaryStream(ctx, query, parameters)
if err != nil {
	return err
}
defer reader.Close()

for reader.Next() {
	row := reader.Row()
	// use row, or stop early
}
if err := reader.Err(); err != nil {
	return err
}
```

`QueryRowBinaryStream` sends `Accept: application/x-hatrie-rowbinary`, checks
the response media type, and decodes rows incrementally. The response body is
owned by the reader and must be closed when iteration stops early. Positional
parameters are passed separately from the SQL text. `QueryRowBinaryIterator`
is retained as a compatibility alias for the same API.

For callback-style code:

```go
n, err := hatSql.QueryRowBinaryRows(ctx, conn, "SELECT id, name", func(row hatSql.Row) error {
	return consume(row)
})
```

The `hatCache` package re-exports this helper as `hatCache.QueryRowBinaryRows`.
The callback helper intentionally uses no parameters; use
`QueryRowBinaryStream` when a parameterized query is required.

## Compatibility And Fallback

The existing JSON and NDJSON APIs remain unchanged and are still the fallback:

```go
result, err := conn.QueryParameters(ctx, query, parameters)
rows, err := hatSql.QueryRows(ctx, conn, query, visit)
iterator, err := hatSql.QueryIterator[hatSql.Row](ctx, conn, query, parameters)
```

Use the existing APIs when a proxy, client, or server does not support the
RowBinary media type. The RowBinary path rejects a successful response with a
different `Content-Type` rather than trying to decode an unrelated payload.

## Measured Tradeoff

The benchmark uses an in-memory HTTP transport and 128 rows with `id` and
`name` columns. It measures client decode work and fixed fixture bytes, not
network latency or server-side query execution. Five `-benchmem` samples ran
on Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median wire bytes | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| NDJSON `QueryRows` | 230,449 | 6,250 | 82,051 | 1,953 |
| RowBinary `QueryRowBinaryRows` | 46,397 | 2,461 | 58,903 | 812 |

For this workload, RowBinary used 2.54x less wire data, ran 4.97x faster,
allocated 28.2% fewer bytes, and performed 58.4% fewer allocations. Reported
throughput improved from a 27.12 MB/s median to 53.04 MB/s median. The result
is workload-dependent: wider types, compression, network behavior, and server
execution time can change the balance. The regular NDJSON path remains
available without a configuration change.

## Verification

The feature is covered by `hat/hatSql/client_row_binary_test.go` for request
negotiation, RowBinary decoding, early callback termination, malformed payloads,
and unexpected media types. The round-6 Makefile targets are:

```text
make test-c192-rowbinary-client
make race-c192-rowbinary-client
make benchmark-c192-rowbinary-client
make vet-c192-rowbinary-client
make compile-c192-rowbinary-client
```
