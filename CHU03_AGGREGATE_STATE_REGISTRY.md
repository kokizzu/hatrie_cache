# CH-U03 Aggregate State Codec Registry

`hatDataStructure.AggregateStateEnvelope` already provides bounded, checksummed
HAG1 wire data. This feature adds an opt-in registry that maps an exact
aggregate kind and version to typed encode/decode callbacks, so workers can
exchange more aggregate implementations without adding a central type switch.

Existing `MarshalAggregateState` and `New*FromAggregateState` APIs are
unchanged. The registry has no package-global state and is not consulted by
normal cache or SQL execution paths.

## Example

```go
registry, err := hatDataStructure.NewDefaultAggregateStateRegistry()
if err != nil {
	return err
}

encoded, err := registry.Marshal(
	hatDataStructure.AggregateStateKindHyperLogLog,
	1,
	hll,
)
if err != nil {
	return err
}

envelope, value, err := registry.Unmarshal(encoded)
if err != nil {
	return err
}
_ = envelope.Kind
restored := value.(hatDataStructure.HyperLogLog)
```

Application codecs use the same contract:

```go
registry.Register(hatDataStructure.AggregateStateCodec{
	Kind:    "my_sum",
	Version: 1,
	Encode: func(value any) ([]byte, error) {
		return hatDataStructure.MarshalAggregateStateEnvelope("my_sum", 1, encodeSum(value))
	},
	Decode: func(data []byte) (any, error) {
		return decodeSum(data)
	},
})
```

`Encode` must return a complete HAG1 envelope whose kind/version exactly match
the registry call. `Decode` receives the complete wire value and remains
responsible for type-specific payload validation.

## Safety And Limits

- `NewDefaultAggregateStateRegistry` registers the built-in HyperLogLog and
  t-digest codecs.
- Registration is exact on `(kind, version)`; duplicates are rejected rather
  than silently replacing a decoder.
- The registry is bounded at 128 codecs and inherits the envelope's 64 MiB
  wire limit, kind validation, version validation, and CRC32 checksum.
- Unknown kinds fail closed with `ErrAggregateStateRegistryCodecMissing`.
- A malformed codec wire value is rejected before a caller can decode it.
- The registry uses an `RWMutex` only around codec lookup/registration; user
  callbacks run outside the lock and may safely call other registry methods.

This is a codec-selection layer, not a trust boundary. Callers must only
register decoders for payloads they are willing to process and should authorize
which kind/version pairs a remote peer may send.

## Benchmark

The benchmark compares the generic registry with direct HAG1 envelope encoding
for the same 8-byte payload, then measures registry decoding. It uses Go's
standard benchmark runner on an AMD Ryzen 9 5950X, five samples per case, with
`-benchmem`.

| Case | Raw samples (ns/op) | Median | Memory |
| --- | --- | ---: | --- |
| Direct HAG1 envelope marshal | 81.51, 80.94, 78.85, 80.14, 80.29 | 80.29 | 56 B/op, 2 allocs/op |
| Registry marshal and exact metadata check | 207.5, 198.5, 202.1, 199.7, 203.7 | 202.1 | 72 B/op, 4 allocs/op |
| Registry unmarshal and typed decode | 191.2, 188.7, 182.1, 186.2, 185.6 | 186.2 | 32 B/op, 4 allocs/op |

The registry marshal path is about `2.52x` the direct envelope marshal time,
with `16 B/op` and two additional allocations for exact codec lookup and wire
metadata verification. The registry unmarshal path costs `187.7 ns/op` and
`32 B/op`; it is the dispatch and typed-decode path, not a raw envelope parse.
The registry is opt-in, so existing typed aggregate APIs do not pay this cost.

Reproduce with:

```text
make benchmark-chu03-aggregate-state-registry
```
