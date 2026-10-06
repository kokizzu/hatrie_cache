# CH-U03 Aggregate State Registry

The HAG1 `AggregateStateEnvelope` already gives partial aggregate data a
bounded, versioned, checksummed wire representation. CH-U03 adds an opt-in
`AggregateStateRegistry` so callers can register typed kind/version codecs once
and then encode, decode, and merge partial states without repeating dispatch
logic at every worker or node boundary.

## Usage

```go
registry := hatDataStructure.NewAggregateStateRegistry()
err := registry.Register(hatDataStructure.AggregateStateCodec{
    Kind:    "orders.sum",
    Version: 1,
    Encode: func(value any) ([]byte, error) {
        return encodeOrderSum(value)
    },
    Decode: func(payload []byte) (any, error) {
        return decodeOrderSum(payload)
    },
    Merge: func(left, right any) (any, error) {
        return mergeOrderSums(left, right)
    },
})

wire, err := registry.Marshal("orders.sum", 1, state)
decoded, err := registry.Unmarshal(wire)
merged, err := registry.Merge(leftWire, rightWire)
```

`Encode`, `Decode`, and `Merge` callbacks run outside the registry lock. A
callback must validate its own typed value and must not retain the payload
slice passed to `Decode`. The registry owns the HAG1 envelope validation,
checksum, kind/version lookup, and bounded wire size.

The zero-value registry is usable. `NewAggregateStateRegistry` defaults to 64
codecs; `NewAggregateStateRegistryWithLimit` accepts a caller limit from 1 to
1024. Duplicate kind/version registration and unknown kind/version data are
rejected. Existing HLL, t-digest, and other aggregate implementations can
remain on their current direct APIs and be wrapped only where typed registry
dispatch is useful.

## Safety and compatibility

- HAG1 continues to reject malformed magic, unsupported wire versions,
  truncated/trailing bytes, checksum failures, invalid names, and oversized
  payloads.
- Registry lookup is keyed by both `Kind` and `Version`; a newer version is
  not silently decoded by an older callback.
- `Merge` requires matching kind/version envelopes and returns a new HAG1
  envelope. The existing direct aggregate paths are unchanged.
- Registry size is bounded and operations use an `RWMutex`; callbacks are not
  invoked while the lock is held, so slow user code cannot block registration
  bookkeeping indefinitely.
- The registry does not execute untrusted code by itself. Applications must
  authenticate peers and decide which codecs are allowed before registering
  callbacks.

## Benchmark

The matched comparison ran through `make codex-chu03-bench` five times per
benchmark on Linux amd64, AMD Ryzen 9 5950X. Both paths used the same 416-byte
payload and HAG1 envelope. The direct rows are the committed pre-registry
baseline benchmark in the same invocation.

| Path | Median ns/op | B/op | allocs/op | Relative to direct path |
| --- | ---: | ---: | ---: | ---: |
| Direct HAG1 marshal | 148.6 | 448 | 1 | 1.00x |
| Registry marshal | 212.0 | 472 | 2 | 1.43x |
| Direct HAG1 unmarshal | 159.6 | 424 | 2 | 1.00x |
| Registry unmarshal | 251.7 | 448 | 3 | 1.58x |
| Registry merge | 412.2 | 152 | 9 | New operation |

The registry adds one allocation and 24 bytes in this small payload case. It
is therefore not enabled on existing hot paths by default. Its value is a
single bounded contract for typed dispatch and exact partial-state merging,
not a claim of lower CPU or memory than direct envelope calls. The larger
existing HLL and t-digest state benchmarks remain covered by their current
direct codecs.

### Raw matched runs

```text
BenchmarkCHU03DirectAggregateStateEnvelopeMarshal: 149.2 147.1 146.9 149.2 148.6 ns/op; 448 B/op; 1 alloc/op
BenchmarkCHU03RegistryAggregateStateMarshal: 215.0 215.2 212.0 210.3 209.9 ns/op; 472 B/op; 2 alloc/op
BenchmarkCHU03DirectAggregateStateEnvelopeUnmarshal: 158.4 158.3 159.6 161.0 167.0 ns/op; 424 B/op; 2 alloc/op
BenchmarkCHU03RegistryAggregateStateUnmarshal: 252.1 251.7 251.1 250.1 255.1 ns/op; 448 B/op; 3 alloc/op
BenchmarkCHU03RegistryAggregateStateMerge: 413.4 421.4 412.2 403.6 410.5 ns/op; 152 B/op; 9 alloc/op
```
