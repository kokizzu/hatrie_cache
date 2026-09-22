# C224: Importable ArgMax/ArgMin Aggregate State

## What changed

`hatDataStructure` now exports two mergeable typed aggregate states:

- `ArgMaxInt64State` keeps the argument paired with the greatest `int64` value.
- `ArgMinInt64State` keeps the argument paired with the smallest `int64` value.

Both states have a useful zero value, allocation-free `Observe` and in-memory
`Merge` operations, and `Result()` returns `(argument, value, ok)`. Equal
ordering values select the lower argument. That tie rule is deterministic and
makes partition-local updates merge consistently regardless of merge order.

The existing SQL `ARGMAX_STATE` and `ARGMIN_STATE` implementation is not
changed by this feature. These typed states are an importable building block
for callers that use integer identifiers, sequence numbers, or Unix timestamps.

## Wire format

`MarshalAggregateState` uses the existing checksummed HAG1 envelope:

| Part | Size | Description |
| --- | ---: | --- |
| HAG1 envelope metadata | 24 bytes | Magic, wire version, kind, state version, payload length, and CRC |
| State payload | 24 bytes | Seen flag, seven reserved zero bytes, signed argument, signed value |
| Complete sample wire value | 48 bytes | `argmax_int64` or `argmin_int64` envelope for the sample state |

Decoding rejects invalid checksums, kinds, versions, payload lengths, seen
flags, non-zero reserved bytes, and non-canonical empty states. Failed
`MergeAggregateState` calls leave the receiver unchanged.

Example:

```go
var latest hatDataStructure.ArgMaxInt64State
latest.Observe(101, 1700000000)
latest.Observe(102, 1700000000) // lower argument 101 remains selected

wire, err := latest.MarshalAggregateState()
restored, err := hatDataStructure.NewArgMaxInt64StateFromAggregateState(wire)
argument, timestamp, ok := restored.Result()
```

## Benchmark

Command:

```text
make benchmark-c224-arg-state
```

Five samples were measured on Linux/amd64, AMD Ryzen 9 5950X. The baseline
was captured before implementation using a JSON object with the same three
logical fields. The HAG1 measurements were captured after implementation.

| Operation | JSON median | HAG1 median | Improvement | JSON memory | HAG1 memory | JSON allocs | HAG1 allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Marshal | 187.9 ns/op | 77.70 ns/op | 2.42x faster | 88 B/op | 48 B/op | 2 | 1 |
| Unmarshal | 895.5 ns/op | 109.3 ns/op | 8.19x faster | 240 B/op | 40 B/op | 5 | 2 |
| Decode and merge | not measured as JSON baseline | 111.4 ns/op | 8.04x vs JSON unmarshal | not comparable | 40 B/op | not comparable | 2 |

The fixed HAG1 wire value is 48 bytes for this state, compared with 51 bytes
for the JSON sample. The main gain is CPU and temporary allocation reduction,
not a large wire-size reduction. The tradeoff is deliberately narrow typing:
arbitrary SQL scalar payloads, floating-point ordering, and string arguments
remain separate future state types rather than being hidden behind a slower
generic representation.

## Verification

```text
make test-c224-arg-state
make verify-c224-arg-state
make race-c224-arg-state
make vet-c224-arg-state
```
