# PGWire Parameter-Description Reuse

Extended-query parameter descriptions now encode directly into the
connection-local backend packet buffer. The exact body size is known from the
OID count, so the reusable path avoids the temporary OID body allocation and
the second copy into the framed packet.

Packets over the existing 64 KiB reuse cap use the old allocating path and do
not enlarge retained connection memory. Ordinary `net.Conn` callers remain
unchanged.

## Benchmark

Fixture: six PostgreSQL parameter type OIDs written to an in-memory
connection. Five-sample medians are shown.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Allocating control | 30.74 | 32 | 1 |
| Direct reusable description | 11.36 | 0 | 0 |

The direct path is 2.71x faster and removes the body allocation. The
pre-change single-run baseline was 32.67 ns/op, 32 B/op, and 1 allocation;
the paired control preserves that allocation profile.

## Verification

```text
make test-pgwire-parameter-description
make race-pgwire-parameter-description
make benchmark-pgwire-parameter-description-baseline
make benchmark-pgwire-parameter-description
```

Tests compare complete wire bytes, repeated buffer reuse, and non-retention of
an oversized OID list.
