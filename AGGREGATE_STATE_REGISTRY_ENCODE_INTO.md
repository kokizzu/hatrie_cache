# Aggregate State Registry EncodeInto

## What changed

`AggregateStateRegistry.EncodeInto` adds an opt-in destination-reuse path for
HAG1 aggregate-state encoding:

```go
wire, err = registry.EncodeInto(wire, "sum", 1, state)
```

When `wire` has enough capacity, the final envelope is written into its
existing backing array. The existing `Encode` method remains unchanged.

The registered codec contract still returns a payload slice, so this change
does not remove the codec's payload allocation. It removes the separate final
envelope allocation and preserves kind/version validation, payload bounds, and
CRC32 checksumming.

`dst` is treated as reusable output: the returned slice starts at index zero,
and callers should retain the returned slice for the next call. On errors the
method returns `nil` and the existing codec error wrapping is preserved.

## Measurement

Workload: the existing `registrySumState` codec, a 32-byte HAG1 wire value,
ten benchmark samples on an AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `registry.Encode` baseline | 125.5 | 48 | 3 | 1.00x |
| Reused `registry.EncodeInto` | 111.6 | 24 | 2 | 1.12x faster |

The reusable path therefore halves envelope-related allocated bytes and removes
one allocation per call. It is opt-in because callers must retain a mutable
destination buffer, and the codec payload allocation remains.

Benchmark command:

```text
make benchmark-aggregate-state-registry
make benchmark-aggregate-state-registry-encode-into
```

## Verification

- A red test first failed because `EncodeInto` was undefined.
- Focused normal and race tests pass.
- Full `hat/hatDataStructure` normal and race tests pass on the clean delivery
  base.
- Wire bytes match `Encode` across empty, varint-boundary, and 64 KiB payloads;
  destination backing reuse and codec error wrapping are covered.
