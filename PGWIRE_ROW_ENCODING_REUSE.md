# PGWire Data-Row Encoding Reuse

The PGWire response writer now encodes `DataRow` fields directly into the
connection-local backend packet buffer. The previous path allocated a row
body, then allocated or copied a framed packet around it. The reusable path
computes the bounded packet size once and writes NULL markers, lengths, and
text bytes in place.

Rows whose complete packet exceeds the 64 KiB backend reuse cap use the old
allocating path and do not cause the connection to retain a large buffer. A
plain `net.Conn` still uses the existing behavior, so this optimization only
changes the `ServeConn` path that owns the reusable connection wrapper.

## Benchmark

Fixture: a three-cell row containing two short text values and one SQL NULL,
written to an in-memory connection. Five samples were run after the change;
medians are shown.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Allocating control | 101.8 | 88 | 4 |
| Direct reusable row encoding | 17.23 | 0 | 0 |

The direct path is 5.91x faster for this row shape and eliminates both the
temporary row body and packet allocations. The pre-change baseline was 89.35
ns/op, 88 B/op, and 4 allocations; the paired control confirms the same
allocation profile. This is a row-encoding microbenchmark, not an end-to-end
network or SQL execution claim.

## Verification

```text
make test-pgwire-row-encoding
make race-pgwire-row-encoding
make benchmark-pgwire-row-encoding-baseline
make benchmark-pgwire-row-encoding
```

Tests cover byte-for-byte framing, repeated buffer reuse, SQL NULL markers,
and non-retention of oversized rows.
