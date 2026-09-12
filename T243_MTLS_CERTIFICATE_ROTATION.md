# Mutual TLS Certificate Rotation

`hatPeer` now exposes an imported, restart-free certificate provider for
authenticated peer listeners. It combines:

- TLS 1.3 as the minimum protocol version;
- `RequireAndVerifyClientCert` for mutual TLS; and
- atomic server-certificate rotation for new handshakes.

## Setup

Create a provider from the initial server certificate and use the generated
configuration on the listener:

```go
provider, err := hatPeer.NewCompactPeerTLSCertificateProvider(serverCertificate)
if err != nil {
    return err
}
tlsConfig, err := hatPeer.NewCompactPeerMutualTLSConfig(provider, clientCAs)
if err != nil {
    return err
}

tcpListener, err := net.Listen("tcp", address)
if err != nil {
    return err
}
listener := tls.NewListener(tcpListener, tlsConfig)
peerListener, err := hatPeer.NewCompactPeerListener(listener, hatPeer.CompactPeerListenerOptions{
    RequireTLS: true,
    Authorize:  authorizePeer,
})
if err != nil {
    return err
}
```

Publish a replacement certificate without restarting the listener:

```go
if err := provider.Rotate(nextServerCertificate); err != nil {
    return err
}
```

Rotations validate every DER chain entry before publication. A failed
rotation leaves the active certificate unchanged. Existing connections keep
the certificate selected by their current handshake; new connections use the
replacement.

## Security Notes

`clientCAs` must contain only the CA certificates trusted for peer clients.
The helper intentionally rejects a nil or empty pool. Certificate rotation
does not revoke already-established sessions and does not change the client CA
pool, so CA changes still require a listener/configuration replacement and a
connection-drain policy. The provider serves one certificate for all SNI
names; use separate listeners or a custom `GetCertificate` policy when peers
need different certificates.

The callback returns provider-owned immutable certificate state to the TLS
package. Callers must not mutate the `tls.Certificate` passed to the provider
after construction or rotation.

## Measurement

Machine: AMD Ryzen 9 5950X, Go `linux/amd64`, five samples,
`-benchtime=1s -benchmem`.

| Path | Median CPU | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Static certificate callback baseline | 0.474 ns/op | 0 B/op | 0 allocs/op |
| Atomic rotating provider lookup | 0.940 ns/op | 0 B/op | 0 allocs/op |
| Static full TLS 1.3 mutual handshake | 793,502 ns/op | 148,202 B/op | 1,153 allocs/op |
| Rotating full TLS 1.3 mutual handshake | 786,266 ns/op | 148,245 B/op | 1,153 allocs/op |

The provider lookup is about `1.98x` the tiny callback baseline, adding about
`0.47 ns` and no allocations. End-to-end handshake medians are within normal
run variance (`0.99x` versus static), with the same allocation count and
effectively identical memory use.

## Verification

```text
make test-t243-tls-rotation
make test-t243-package
make race-t243
make vet-t243
make benchmark-t243
```
