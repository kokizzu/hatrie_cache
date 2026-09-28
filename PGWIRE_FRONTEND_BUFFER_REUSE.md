# PGWire Frontend Message Buffer Reuse

`hatPgWire.ServeConn` now reuses one per-connection payload buffer while it reads frontend PostgreSQL protocol messages. This removes the normal allocation of a new body slice for every message without changing the public server API or the ownership rules of parsed messages.

The reusable buffer is capped at 64 KiB. A larger message is read into a temporary slice and is not retained as the connection's reusable capacity. That keeps an occasional large bind or copy payload from permanently inflating memory retained by an otherwise small-message connection.

## Why

The change applies the same ownership principle used by high-throughput systems:

- ClickHouse keeps work in reusable blocks instead of allocating per row.
- Materialize processes batches of updates through a dataflow rather than one allocation per record.
- Tarantool's iproto path is designed around compact, reusable request and response buffers.

For PostgreSQL frontend traffic, the smallest compatible version of that idea is a connection-local buffer. It is deliberately not a shared pool, so concurrent connections cannot race and one connection cannot retain another connection's data.

## Benchmark

Fixture: 128 `Q` messages, each with a 256-byte payload, read from an in-memory connection. Five benchmark samples were run after the implementation; the median is shown.

| Reader | Median time | Bytes allocated | Allocations | Reuse improvement |
| --- | ---: | ---: | ---: | ---: |
| Allocating reader | 11,435 ns/op | 33,450 B/op | 256 allocs/op | 1.00x |
| Reusable reader | 4,150 ns/op | 938 B/op | 129 allocs/op | 2.76x faster, 35.7x fewer bytes, 1.98x fewer allocations |

The benchmark measures protocol-body reading only. It does not claim that all request handling becomes 2.76x faster; parsing, authentication, application work, and network I/O are outside this microbenchmark.

## Verification

Focused correctness test:

```text
make test-pgwire-buffer-reuse
```

Focused race test:

```text
make race-pgwire-buffer-reuse
```

Benchmark:

```text
make benchmark-pgwire-buffer-baseline
make benchmark-pgwire-buffer-reuse
```

The existing `readFrontendMessage` helper remains available for tests and callers that want an independently allocated body. The live connection path uses `readFrontendMessageInto` and updates its reusable capacity after each successful read.
