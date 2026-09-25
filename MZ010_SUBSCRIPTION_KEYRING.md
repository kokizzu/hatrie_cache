# MZ-010 Subscription Wire Keyring

`SQLSubscriptionWireKeyring` adds bounded HMAC key rotation for the existing
`SQLSubscriptionWireEnvelope`. It seals new frames with one active key and
accepts up to four previous keys while a fleet transitions credentials.

The existing `SealSQLSubscriptionWireEnvelope` and
`OpenSQLSubscriptionWireEnvelope` functions remain unchanged. The keyring does
not add a key identifier or change `HSE1` wire bytes, so old clients and
persisted frames remain compatible. Its published key set is immutable and
read through an atomic pointer; normal active-key `Open` and `Seal` calls do
not take a mutex. Rotation is serialized and copies caller-provided key
buffers.

## API

```go
ring, err := hatSql.NewSQLSubscriptionWireKeyring(activeKey)
if err != nil {
    return err
}
if err := ring.Rotate(nextKey); err != nil {
    return err
}
wire, err := ring.Seal(envelope)
decoded, err := ring.Open(wire)
```

`Open` tries the active key first, then the bounded previous-key grace window.
Malformed frames fail immediately; unknown valid keys return the existing
authentication error. Rotation evicts the oldest key after the four-key bound.

## Measurement

Command: `make benchmark-mz010-subscription-keyring`, 200 ms per benchmark on
Linux/amd64, AMD Ryzen 9 5950X.

| Path | ns/op | B/op | allocs/op | Tradeoff |
| --- | ---: | ---: | ---: | --- |
| Direct envelope open | 680.7 | 592 | 8 | Existing control |
| Keyring, active key | 678.5 | 592 | 8 | No allocation/heap overhead; CPU within benchmark noise |
| Keyring, previous key | 1,330 | 1,104 | 14 | 1.95x CPU, +512 B/op, +6 allocs during grace-window reads |

The previous-key cost is bounded by four extra HMAC attempts and disappears
after clients switch to the active key. This is an operational/security
capability rather than a default throughput change.

## Verification

```text
make test-mz010-subscription-keyring
make benchmark-mz010-subscription-keyring
```

Tests cover round trips across rotation, bounded eviction, malformed frames,
and invalid keyring configuration.
