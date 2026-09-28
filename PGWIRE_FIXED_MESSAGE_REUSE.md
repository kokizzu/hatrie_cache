# PGWire Fixed-Control Message Reuse

Fixed-size PostgreSQL control messages now write directly into the
connection-local backend packet buffer: authentication OK, cleartext-password
challenge, ready-for-query, and backend key data. This removes temporary body
slices from the reusable `ServeConn` path while preserving the ordinary
`net.Conn` fallback.

## Benchmark

Five-sample medians for writes to an in-memory connection:

| Message | Allocating ns/op | Reusable ns/op | Allocating B/op | Reusable B/op | Allocating allocs/op | Reusable allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Authentication OK | 23.42 | 5.44 | 16 | 0 | 1 | 0 |
| Ready for query | 20.13 | 4.75 | 8 | 0 | 1 | 0 |

The reusable path is approximately 4.3x faster for authentication OK and
4.2x faster for ready-for-query. The pre-change single-run baselines were
22.17 ns/op and 18.54 ns/op; the paired controls preserve the original
allocation profiles. This is a small control-message microbenchmark, not an
end-to-end connection setup claim.

## Verification

```text
make test-pgwire-fixed-message
make race-pgwire-fixed-message
make benchmark-pgwire-fixed-message-baseline
make benchmark-pgwire-fixed-message
```

Tests compare complete wire bytes for repeated control messages and backend
key data, including packet-buffer reuse.
