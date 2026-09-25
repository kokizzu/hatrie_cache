# MZ-010 Cross-Process Subscription Transport

`SQLSubscriptionWireTransport` adds an opt-in framed transport for moving
authenticated SQL subscription envelopes across a caller-owned `net.Conn`.
It is intended for process boundaries; the local subscription APIs and their
default execution path are unchanged.

## Wire Contract

Each message is encoded as:

```text
4-byte big-endian frame length
HSE1 authenticated SQL subscription envelope
```

The existing `SQLSubscriptionWireKeyring` can be passed as the
`SQLSubscriptionWireCodec`, so the HSE1 envelope remains authenticated and
key rotation remains bounded. A frame length is validated before allocation
and is limited by `MaxSQLSubscriptionWireTransportFrameBytes`.

## Example

```go
keyring, err := hatSql.NewSQLSubscriptionWireKeyring(activeKey, previousKeys)
if err != nil {
    return err
}

transport := hatSql.NewSQLSubscriptionWireTransport(conn, keyring)
if err := transport.Send(ctx, envelope); err != nil {
    return err
}

received, err := transport.Receive(ctx)
if err != nil {
    return err
}
_ = received
```

The transport owns neither dialing nor listening. Callers remain responsible
for connection establishment, authentication outside the HSE1 envelope,
timeouts, close policy, and reconnect/backoff behavior. Concurrent sends and
receives are serialized independently; a single transport may therefore be
used by one writer and one reader concurrently.

`Send` and `Receive` accept contexts. Cancellation and deadlines interrupt
blocked socket operations and return the context error. Malformed, empty, or
oversized frames are rejected without unbounded allocation.

## Cost

The benchmark uses a deterministic in-memory `net.Conn` to isolate framing and
codec work. It does not model kernel, TLS, or network latency. Results below
are from an AMD Ryzen 9 5950X, Linux/amd64, with `-benchtime=200ms`:

| Path | ns/op | B/op | allocs/op | Relative to direct active-key open |
| --- | ---: | ---: | ---: | --- |
| Direct active-key envelope open | 678.5 | 592 | 8 | 1.00x CPU, 1.00x bytes, 1.00x allocs |
| Transport send | 678.6 | 668 | 9 | 1.00x CPU, +76 B, +1 alloc |
| Transport receive | 762.8 | 744 | 11 | 1.12x CPU, +152 B, +3 allocs |

The transport is deliberately opt-in: callers that already exchange envelopes
through another framing layer can keep using the codec directly and avoid the
additional frame prefix and transport bookkeeping.

## Verification

```text
make test-mz010-subscription-transport
make race-mz010-subscription-transport
make vet-mz010-subscription-transport
make benchmark-mz010-subscription-transport
```

The focused tests cover round trips, concurrent read/write use, context
deadlines, malformed and oversized frames, codec failures, and short socket
writes.
