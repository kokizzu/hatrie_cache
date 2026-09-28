# PGWire Row-Description Encoding Reuse

PGWire row descriptions now write their column metadata directly into the
connection-local backend packet buffer. This avoids growing a temporary body
slice and then copying that body into a second framed packet.

The implementation computes the exact bounded packet size from field names,
writes PostgreSQL table/type/format metadata in place, and falls back to the
existing allocating encoder for packets over 64 KiB. Ordinary `net.Conn`
callers keep the old behavior; `ServeConn` owns the reusable path.

## Benchmark

Fixture: three column descriptors (`int8`, `text`, and `text`) written to an
in-memory connection. Five post-change samples are summarized by median.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Allocating control | 197.6 | 328 | 6 |
| Direct reusable description | 29.03 | 0 | 0 |

The direct path is 6.80x faster for this descriptor shape and removes all
temporary heap allocation. The pre-change baseline was 175.7 ns/op, 328 B/op,
and 6 allocations; the paired control confirms the same allocation profile.
This is a framing/encoding microbenchmark and excludes network and SQL work.

## Verification

```text
make test-pgwire-row-description
make race-pgwire-row-description
make benchmark-pgwire-row-description-baseline
make benchmark-pgwire-row-description
```

Tests compare the complete wire bytes with the allocating encoder, verify
repeated buffer reuse, preserve default text OIDs, and verify oversized
descriptions are not retained.
