# TR043C: Compact Peer Response Schemas

## Status

Implemented as an opt-in extension to prepared compact peer calls. Existing
sessions, frames, and defaults remain unchanged unless response schemas are
explicitly enabled.

## API

```go
template, err := hatPeer.NewCompactRequestTemplateWithResponseSchema(
    []byte("GET_USER"),
    17,
)
session, err := hatPeer.NewCompactPeerSession(conn, hatPeer.CompactPeerSessionOptions{
    EnableResponseSchemas: true,
})
response, err := session.CallTemplate(ctx, template, payload)
```

For listener-managed connections, set `EnableResponseSchemas` on the session
options. `NewCompactPeerListener` advertises
`CompactPeerFeatureResponseSchemas`, and
`CompactPeerSessionOptionsForNegotiatedHandshake` disables the option when the
remote endpoint does not advertise it. Direct sessions must opt in on both
ends themselves.

## Wire Format

The response schema ID is an opaque positive `uint64`. When present, the
compact frame sets `CompactFrameFlagResponseSchema` (bit 2) and writes the
schema ID immediately after the request ID and before the command length.
The ID is encoded as a uvarint and is bounded by the existing frame-size
limits.

Legacy frames have schema ID zero, do not set the new flag, and use the exact
existing layout. The feature is therefore safe to leave disabled for old
peers and mixed deployments. A schema-bearing call sent through a session
that did not negotiate the feature is rejected instead of being silently
interpreted with the wrong response shape.

Handlers receive `ResponseSchemaID` on the request. If a handler returns a
zero schema ID, the session echoes the request ID automatically. Returning a
different non-zero ID is allowed at the protocol layer but is rejected by the
prepared caller as `ErrCompactPeerResponseSchemaMismatch`.

## Cost And Tradeoff

The default path has no new schema field on the wire and keeps the existing
allocation profile. The opt-in path adds one varint byte for schema ID `17` in
the measured request fixture, so a request/response round trip adds two wire
bytes. Larger IDs consume more varint bytes.

Measured with `make benchmark-tr043c-response-schema` on Linux/amd64,
AMD Ryzen 9 5950X, five samples per benchmark:

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Legacy prepared frame marshal | 27.69 | 0 | 0 | 1.00x |
| Schema ID 17 prepared frame marshal | 30.61 | 0 | 0 | 1.11x |
| Legacy end-to-end prepared call | 6,370 | 448 | 7 | baseline |
| Schema ID 17 end-to-end prepared call | 5,995 | 448 | 7 | within net.Pipe variance |

The isolated encoder cost is about 2.92 ns per frame in this fixture. The
end-to-end session benchmark is dominated by `net.Pipe` scheduling and does
not establish a meaningful CPU win or loss; allocation behavior is identical.
The feature is useful when a caller can use a stable schema ID to avoid
repeated response-shape interpretation, not as a general compression feature.

## Verification

Focused schema tests cover frame round trips, prepared calls, unsupported
peers, mismatched IDs, and negotiated listener options. The focused test,
race test, and vet test pass. The existing `TestCompactProtocolRoundTrip`
size assertion also fails on `origin/master` with the same 77-byte result; it
is a pre-existing baseline issue and is not changed by this feature.
