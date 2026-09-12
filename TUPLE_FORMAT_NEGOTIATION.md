# Tuple Format Version Negotiation

Tarantool-style tuple formats now have an opt-in capability exchange for
rolling upgrades. `hatDataStructure.TupleFormat` can advertise its version and
physical field shape, peers can exchange the compact `HTF1` representation, and
the highest exact common version can be selected without decoding a tuple.

The feature does not change existing tuple packing, validation, or peer-session
behavior. It is a library contract; callers send the encoded capability bytes
over their existing peer stream or protocol.

## Example

```go
v1, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
	{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	{Name: "region", Type: hatDataStructure.TupleFieldString},
})
if err != nil {
	return err
}
v2, err := hatDataStructure.NewTupleFormat(2, v1.Fields())
if err != nil {
	return err
}

localV1, err := v1.Capability("orders")
if err != nil {
	return err
}
localV2, err := v2.Capability("orders")
if err != nil {
	return err
}

local := []hatDataStructure.TupleFormatCapability{localV1, localV2}
wire, err := hatDataStructure.MarshalTupleFormatCapabilities(local)
if err != nil {
	return err
}
remote, err := hatDataStructure.UnmarshalTupleFormatCapabilities(wire)
if err != nil {
	return err
}
chosen, err := hatDataStructure.NegotiateTupleFormat(local, remote)
if err != nil {
	return err
}
fmt.Println(chosen.Name, chosen.Version)
// orders 2
```

`Capability` fingerprints the ordered field names, field types, and nullability
flags. The schema version, defaults, and generated callbacks are excluded from
the fingerprint. Therefore two versions with the same physical tuple shape can
be advertised together, while a changed field shape cannot accidentally match.
The fingerprint is a compatibility identifier, not an authentication or
integrity mechanism.

`NegotiateTupleFormat` requires matching name, version, and fingerprint. It
returns `ErrTupleFormatNoCompatibleVersion` when no exact match exists. Multiple
common names are resolved deterministically by highest version, then name, then
fingerprint bytes.

## Wire Format

`MarshalTupleFormatCapabilities` is the default compact representation:

| Part | Encoding |
| --- | --- |
| Magic | ASCII `HTF1` |
| Wire version | One byte, currently `1` |
| Count | Unsigned varint |
| Name length and name | Unsigned varint plus UTF-8 bytes |
| Format version | Unsigned varint |
| Shape fingerprint | 16 bytes |

The decoder is strict. It rejects unknown wire versions, truncated or
overflowing varints, invalid UTF-8 or whitespace-padded names, duplicate named
versions, oversized values, and trailing bytes. Bounds are 1,024 capabilities,
256 bytes per name, and 1 MiB per encoded advertisement.

For a human-readable or legacy integration fallback, callers can encode the
same `TupleFormatCapability` slice with `encoding/json` and choose that transport
at their protocol boundary. JSON is not silently accepted by the HTF1 decoder.

## Rolling Upgrade

1. Keep the old and new capabilities in the advertisement while old peers may
   still connect.
2. Exchange capabilities over the already authenticated peer channel.
3. Require a successful `NegotiateTupleFormat` result before sending tuples.
4. Retire the old capability only after the old peer population is gone.

If a field is added, removed, reordered, or has its type/nullability changed,
the fingerprint changes. That forces an explicit migration or a retained old
format instead of an unsafe downgrade.

## Measurements

Measured on Linux/amd64, AMD Ryzen 9 5950X, with
`make benchmark-t-u40` (`go test -benchmem -count=5`). The workload contains
three capabilities for one named format. JSON is the `encoding/json` baseline
for the same Go values, not a previous Hatrie wire implementation.

| Operation | HTF1 binary | JSON baseline | Result |
| --- | ---: | ---: | --- |
| Marshal | 258 ns/op, 168 B/op, 2 allocs/op | 1,162 ns/op, 400 B/op, 3 allocs/op | 4.5x faster; 2.4x lower allocated bytes |
| Unmarshal | 268 ns/op, 176 B/op, 5 allocs/op | 6,613 ns/op, 624 B/op, 14 allocs/op | 24.6x faster; 3.5x lower allocated bytes; 2.8x fewer allocs |
| Payload | 78 bytes | 319 bytes | 4.1x smaller |
| Negotiate | 462 ns/op, 48 B/op, 1 alloc/op | N/A | bounded exact-match selection |

The encoder benchmark also included a small fast path for already canonical
input. Before that path, the same binary marshal measured about 392 ns/op,
416 B/op, and 6 allocs/op. Afterward it measured 258 ns/op, 168 B/op, and 2
allocs/op. Unsorted input is copied and sorted so deterministic output and the
no-input-mutation guarantee remain intact.

## Verification

```text
make test-t-u40
make benchmark-t-u40
```

Tests cover highest-version selection, name and shape mismatch rejection,
deterministic round trips, malformed wire data, zero fingerprints, and empty
negotiation.
