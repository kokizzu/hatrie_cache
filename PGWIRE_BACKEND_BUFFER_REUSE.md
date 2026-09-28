# PGWire Backend Message Buffer Reuse

`hatPgWire.ServeConn` now wraps each accepted connection in a bounded backend
message writer. Responses up to 64 KiB reuse one connection-local packet
buffer, so the common small response path does not allocate a new framing
slice for every PostgreSQL message.

The buffer is local to one connection and is never shared between goroutines
or clients. A packet larger than the 64 KiB cap is allocated transiently and
is not retained. The existing `writeMessage` behavior for an ordinary
`net.Conn` remains available for tests and callers outside `ServeConn`.

This is the transport-side version of a few established design ideas:

- ClickHouse keeps wire work in reusable blocks rather than allocating per
  row or packet.
- Materialize amortizes dataflow batch ownership across records.
- Tarantool's iproto path favors compact framed messages and bounded reusable
  request/response storage.

## Benchmark

Fixture: one 256-byte backend body written to an in-memory `net.Conn`. The
five-sample benchmark was run after the implementation; medians are shown.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Allocating control | 78.22 | 288 | 1 |
| Reusable connection buffer | 11.10 | 0 | 0 |

The reusable path is 7.05x faster in this framing-only benchmark and removes
the packet allocation entirely. The pre-change one-sample baseline was 74.14
ns/op, 288 B/op, and 1 alloc/op; the paired post-change control confirms the
same allocation profile. Network syscalls, query execution, row encoding, and
application handler work are outside this microbenchmark.

## Verification

```text
make test-pgwire-response-buffer
make race-pgwire-response-buffer
make benchmark-pgwire-response-buffer-baseline
make benchmark-pgwire-response-buffer
```

The tests verify byte-for-byte framing, buffer reuse, and non-retention of an
oversized packet.
