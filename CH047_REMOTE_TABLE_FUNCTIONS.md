# CH-47: URL and S3 table functions

Status: implemented and benchmarked.

`hatSql` table functions previously required every caller to write its own
remote-file adapter. The opt-in `RemoteTableFunctionResolver` adds bounded
`url()` and `s3()` functions while leaving existing table functions and sources
unchanged.

## Usage

```go
resolver, err := hatSql.NewRemoteTableFunctionResolver(base, hatSql.RemoteTableFunctionResolverOptions{
    S3Endpoint:       "https://s3.example.test",
    MaxResponseBytes: 16 << 20,
    MaxRows:          1_000_000,
})
```

The SQL forms are:

```sql
FROM TABLE(url('https://example.test/data.csv', 'csv'))
FROM TABLE(url('https://example.test/data.csv', 'csv', 0, 4096))
FROM TABLE(s3('bucket', 'path/data.ndjson', 'ndjson'))
FROM TABLE(s3('bucket', 'path/data.ndjson', 'ndjson', 0, 4096))
```

The supported formats are `csv`, `json`, and `ndjson` (also `jsonl`). CSV uses
the first row as unique column names and returns string fields. JSON accepts one
object or an array of objects; NDJSON accepts one object per line and decodes
JSON numbers using the standard Go JSON representation.

The optional `start, length` arguments produce an HTTP `Range: bytes=start-end`
request. The selected range must contain complete records for the chosen
format. A server that ignores a non-zero range is rejected instead of silently
returning the wrong rows.

## Limits and security

- The resolver is opt-in; there is no change to ordinary SQL or existing table
  function behavior.
- The default response limit is 16 MiB and the default decoded-row limit is
  1,000,000. Both are configurable but bounded by constructor validation.
- Only HTTP and HTTPS URLs are accepted. URL credentials and control
  characters are rejected.
- The default client has a 30-second timeout and does not follow redirects.
  A custom `HTTPClient` owns authentication, headers, redirect policy, and any
  host/IP allowlist required by the deployment.
- S3 uses path-style endpoint mapping and is intended for public objects unless
  the supplied client adds authentication. The resolver does not log URLs or
  response bodies.
- The wrapper delegates ordinary sources and unknown table functions to its
  base resolver. Optional resolver interfaces are not dynamically forwarded by
  Go interface embedding; use the remote resolver at a boundary where that
  optimization choice is explicit.

## Benchmark

The benchmark uses a deterministic in-process HTTP transport with a 2,048-row
CSV object. It compares decoding the complete 31,667-byte object with decoding
the first 256 rows from a 3,739-byte range. It excludes network latency and
measures the local work and response bytes saved by a selective range.

| Path | Median ns/op | Wire bytes | Median B/op | Median allocs/op | Improvement vs full |
| --- | ---: | ---: | ---: | ---: | --- |
| Full object | 981,341 | 31,667 | 986,640 | 12,343 | 1.00x |
| Bounded range | 139,174 | 3,739 | 125,721 | 1,587 | 7.05x faster, 8.47x lower wire, 7.85x lower B/op, 7.78x fewer allocs |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-47-s3-and-url-table-functions).
