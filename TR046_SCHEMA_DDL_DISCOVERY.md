# TR-46: Schema and DDL Discovery Protocol

`hatSchema.SchemaDiscovery` is an opt-in, transport-neutral control-plane
record for clients, replicas, and operators that need to agree on the schema
and supported DDL operations before sending schema-dependent data.

## Why

The existing schema fingerprint and rolling-compatibility helpers detect
drift, but callers still need a small, bounded record to exchange before
deciding whether a peer can proceed. Sending JSON metadata for every
handshake costs more CPU, memory, and bandwidth than this control-plane
record needs.

## API

```go
local, err := hatSchema.NewSchemaDiscovery(
	schema,
	hatSchema.DefaultSchemaDiscoveryProtocolVersion,
	[]string{"add_nullable_column", "relax_not_null"},
)
if err != nil {
	return err
}

wire, err := local.MarshalBinary()
if err != nil {
	return err
}

var peer hatSchema.SchemaDiscovery
if err := peer.UnmarshalBinary(wire); err != nil {
	return err
}

report, err := hatSchema.CompareSchemaDiscovery(
	local,
	peer,
	[]string{"add_nullable_column"},
)
if err != nil {
	return err
}
if !report.Compatible {
	// Inspect ProtocolMatch, SchemaMatch, NeedsSchemaTransfer, and
	// MissingDDLCapabilities before sending schema-dependent data.
}
```

`NewSchemaDiscovery` validates the complete schema, computes its existing
deterministic fingerprint, copies capabilities, rejects duplicates, and sorts
them. `CompareSchemaDiscovery` treats a protocol mismatch and a missing
required capability as incompatible. A matching protocol with a different
schema sets `NeedsSchemaTransfer`, allowing the caller to fetch a full schema
or run the existing rolling-compatibility check instead of guessing.

## Wire format and limits

The `HSD1` frame contains a one-byte wire version, unsigned-varint protocol and
schema versions, one fingerprint, and sorted length-prefixed capability
strings. It is deliberately not a DDL executor and does not contact a peer.
The caller chooses the transport and policy.

Decoder limits are fixed and applied before allocations:

- 64 KiB maximum frame
- 128 maximum DDL capabilities
- 256 bytes maximum per fingerprint or capability
- no trailing bytes, duplicate capabilities, invalid UTF-8, or non-canonical
  capability ordering

These bounds make the decoder suitable for untrusted handshake input. The
legacy schema, JSON, replication, and tuple paths are unchanged; adoption is
explicit through `MarshalBinary`/`UnmarshalBinary`.

## Tradeoffs

The binary frame is smaller and faster for this metadata shape, but it is a
new protocol contract that callers must version and route. It carries a schema
fingerprint rather than the full schema, so a mismatch still needs an explicit
schema transfer. Capability names are caller-defined and therefore require a
shared registry or documented policy. There is no automatic DDL execution or
network integration by design.

See [BENCHMARK.md](BENCHMARK.md#tr-46-schema-and-ddl-discovery-protocol) for
raw five-sample measurements against the JSON representation.
