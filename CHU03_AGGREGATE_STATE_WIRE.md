# CH-U03 Versioned Partial Aggregate Wire States

CH-U03 completes the generic dispatch layer around the existing HAG1 partial
aggregate envelope. It is opt-in: existing typed HLL, t-digest, and direct
envelope callers keep their current APIs and wire format.

## Usage

Register one codec for each aggregate kind and schema version. The callbacks
own the typed state representation; the registry owns HAG1 framing, bounds,
and checksums.

```go
codec, err := hatDataStructure.NewAggregateStateCodec(
    "sum",
    1,
    func(state interface{}) ([]byte, error) { return encodeSum(state) },
    func(payload []byte) (interface{}, error) { return decodeSum(payload) },
)
if err != nil {
    return err
}

registry := hatDataStructure.NewAggregateStateRegistry()
if err := registry.Register(codec); err != nil {
    return err
}

wire, err := registry.Encode("sum", 1, partialState)
if err != nil {
    return err
}
envelope, decodedState, err := registry.Decode(wire)
```

Kinds use the existing lowercase HAG1 grammar (`a-z`, digits, `.`, `_`, and
`-`). A `(kind, version)` pair can be registered only once. Decode rejects an
unknown kind or unsupported version before invoking a callback.

## Envelope And Limits

The registry reuses HAG1: marker `HAG1`, envelope version `1`, kind, aggregate
schema version, payload length, payload, and CRC32 IEEE checksum. The decoder
rejects malformed varints, trailing bytes, checksum failures, invalid kinds,
and payloads beyond the existing 64 MiB envelope bound. The payload passed to
the decode callback is detached from the input buffer.

HAG1 provides integrity and bounded allocation, not encryption or transport
authentication. Use an authenticated transport when state crosses a trust
boundary. The direct envelope API remains available for forward-compatible
unknown kinds; the registry intentionally requires an exact registered codec.

## Measured Tradeoff

Machine: AMD Ryzen 9 5950X, linux/amd64. Five samples per benchmark. The
fixture is a small signed sum state; JSON is the standard-library metadata and
state baseline.

| Operation | Direct HAG1 range ns/op | Registry HAG1 range ns/op | JSON range ns/op | Registry vs JSON | Direct HAG1 B/op | Registry B/op | JSON B/op | Direct allocs/op | Registry allocs/op | JSON allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Encode | 65.1-66.8 | 134.5-147.2 | 182.8-197.8 | 1.41x faster | 24 | 48 | 80 | 1 | 3 | 2 |
| Decode | 73.9-83.9 | 137.3-148.6 | 780.9-854.6 | 5.90x faster | 16 | 24 | 256 | 2 | 3 | 6 |

The registry costs about 2.1x encode time and 1.8x decode time versus direct
HAG1 for this tiny state because callback dispatch and typed payload creation
add work. It still materially beats JSON while providing kind/version
selection and compatibility errors. Larger payloads reduce the fixed dispatch
share; measure application-specific states before making it a hot-path
default.

Raw benchmark command:

```text
make prepare-chu03-commit
make benchmark-chu03-aggregate-registry-clean
```

The complete samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-u03-versioned-partial-aggregate-wire-states).
