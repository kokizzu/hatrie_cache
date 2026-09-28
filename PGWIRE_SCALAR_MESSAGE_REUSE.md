# PGWire Scalar Message Reuse

PGWire startup parameter-status messages and command-complete tags now encode
their C strings directly into the connection-local backend packet buffer.
This removes the temporary C-string body and the extra framed-packet
allocation from common scalar responses.

The reusable encoder is bounded by the existing 64 KiB packet cap. Oversized
values use the prior allocating path and are not retained. Ordinary
`net.Conn` callers keep the existing behavior; `ServeConn` uses the reusable
connection wrapper.

## Benchmark

Five-sample medians for short protocol values written to an in-memory
connection:

| Message | Allocating ns/op | Reusable ns/op | Allocating B/op | Reusable B/op | Allocating allocs/op | Reusable allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Parameter status | 72.22 | 10.46 | 80 | 0 | 3 | 0 |
| Command tag | 55.77 | 6.94 | 40 | 0 | 3 | 0 |

That is approximately 6.90x faster for parameter status and 8.04x faster for
command tags. The pre-change single-run baselines were 66.48 ns/op and 54.52
ns/op respectively; the paired controls preserve the original allocation
profiles. This is a framing microbenchmark, not an end-to-end network claim.

## Verification

```text
make test-pgwire-scalar-message
make race-pgwire-scalar-message
make benchmark-pgwire-scalar-message-baseline
make benchmark-pgwire-scalar-message
```

Tests compare complete wire bytes, repeated buffer reuse, and oversized-value
non-retention.
