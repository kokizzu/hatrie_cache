# HTTP SQL RowBinary Streaming and Import

Hatrie-cache supports an opt-in, self-describing binary stream for large
SQL result sets. It is inspired by ClickHouse RowBinary, but uses a small
`HRS1` prelude so a client can validate the format and result schema before
reading rows.

## Usage

The request is the same read-only SQL request used by the JSON and NDJSON
paths. Set `stream` to `true` and advertise the binary media type:

```sh
curl -sS \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/x-hatrie-rowbinary' \
  --data '{"query":"SELECT id, name FROM CACHE users","stream":true}' \
  http://127.0.0.1:8080/api/sql
```

The Go client exposes the same path without buffering the complete result:

```go
reader, err := hatSql.QueryRowBinaryIterator(ctx, conn,
    "SELECT id, name FROM CACHE users", nil)
if err != nil {
    return err
}
defer reader.Close()

for reader.Next() {
    row := reader.Row()
    // Consume row here; the reader advances incrementally.
    _ = row
}
return reader.Err()
```

When `Accept` is absent, or when the binary media type has `q=0`, the
existing `application/x-ndjson` streaming response remains unchanged. This
feature does not change materialized JSON responses, SQL semantics, auth,
RBAC, pagination, or the default wire format.

Bulk import uses the same self-describing stream in the opposite direction.
The `query` URL parameter supplies only the target column list; it must be an
`INSERT INTO CACHE(...)` target without `VALUES` or `SELECT`. Incoming columns
are mapped positionally, just like the existing `INSERT ... SELECT` path:

```sh
curl -sS \
  -H 'Content-Type: application/x-hatrie-rowbinary' \
  --data-binary @rows.hrs \
  'http://127.0.0.1:8080/api/sql/import?query=INSERT%20INTO%20CACHE%28key%2C%20value%29'
```

The embedded API is useful when the source is already an `io.Reader`:

```go
result, err := hatCache.ExecuteSQLRowBinaryInsert(ctx, trie,
    "INSERT INTO CACHE(key, value)", input,
    hatCache.SQLRowBinaryImportOptions{})
```

The default batch size is 4,096 rows, configurable through
`SQLRowBinaryImportOptions.BatchSize` or the HTTP `batch_size` query
parameter. Each batch is atomic and the result reports `Affected` and
`Batches`; a later decode or write error can leave earlier completed batches
applied. HTTP imports are write operations, so monitoring auth, SQL RBAC,
write protection, maintenance read-only mode, and the 1 GiB default body cap
apply. The body cap can be changed with
`MonitoringOptions.SQLRowBinaryImportMaxBytes`.

## Wire Format

The stream is ordered as follows:

1. Four ASCII magic bytes: `HRS1`.
2. An unsigned varint containing the JSON schema-header length.
3. The UTF-8 JSON schema header:

   ```json
   {
     "format": "hatrie-rowbinary",
     "version": 1,
     "columns": [
       {"name":"id","type":1,"nullable":true},
       {"name":"name","type":5,"nullable":true}
     ]
   }
   ```

4. Each row, in declared column order. Nullable values start with one marker:
   `1` means NULL and `0` means a non-NULL value. Non-NULL values use the
   existing bounded SQL RowBinary codec for their declared type. Non-nullable
   values have no marker.

The header is emitted even when the result has zero rows. A stream is limited
to the existing SQL row limit, a 1 MiB schema header, and 64 MiB for one
variable-length value. Readers must check `Err()` after `Next()` returns
`false`; a truncated stream is reported there. Once an HTTP response has
started, a transport failure cannot be replaced by a second JSON error body.

Type inference uses native SQL values where possible. A column whose first
observed value is NULL is conservatively declared `JSON`, allowing later
values to remain representable. Empty-result schemas use the SQL engine's
declared column metadata and are therefore preferable when no rows are
available to infer from.

## Benchmark

The benchmark encodes the same 2,048-row result (`int64`, repeated strings,
booleans, and byte payloads) with Go amd64 on the same host, using
`-count=5`. The baseline is the pre-feature commit's NDJSON stream; the new
path is the current RowBinary stream.

| Metric | NDJSON baseline | RowBinary stream | Improvement |
| --- | ---: | ---: | ---: |
| Median CPU per result | 1,890,697 ns | 355,768 ns | 5.31x faster |
| Encoded result size | 187,007 B | 63,843 B | 2.93x smaller |
| Allocated bytes | 1,035,890 B | 228,729 B | 4.53x lower |
| Allocations | 20,516 | 5,912 | 3.47x lower |

Representative raw benchmark lines:

```text
BenchmarkCH050NDJSONBaseline-32 644 1890697 ns/op 187007 bytes/result 1035890 B/op 20516 allocs/op
BenchmarkCH050RowBinaryStream-32 3325 355768 ns/op 63843 bytes/result 228729 B/op 5912 allocs/op
```

The binary path is deliberately opt-in. It adds a schema prelude and one
NULL marker per column per row, and JSON fallback for unknown or NULL-first
types can reduce the size advantage for those columns. NDJSON remains the
right default for clients that require the existing JSON envelope or
human-readable diagnostics.

## Verification

Use the repository targets so the checks are repeatable:

```sh
make test-ch050
make race-ch050
make vet-ch050
make benchmark-ch050-before
make benchmark-ch050-after
```

The focused tests cover typed values, NULLs, empty results, malformed/truncated
input, all RowBinary scalar types, bounded import batches, HTTP content
negotiation and write protection, the Go client iterator, and compatibility
with the existing NDJSON path. Import and stream benchmarks are run with
`make benchmark-ch050-rowbinary-import` and payload sizing with
`make measure-ch050-rowbinary-import`.
