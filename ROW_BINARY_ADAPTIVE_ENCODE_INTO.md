# Adaptive RowBinary EncodeInto

## What changed

`EncodeSQLRowBinaryAdaptiveInto` adds an opt-in reusable output buffer for the
HSA1 adaptive RowBinary envelope:

```go
wire, err = hatSql.EncodeSQLRowBinaryAdaptiveInto(wire, columns, rows)
```

The function keeps the existing legacy, first-order delta, and second-order
delta candidate encoders and uses the same shortest-candidate tie-breaking.
Only the final selected envelope is written into `dst`; the candidate payload
allocations remain unchanged. The existing `EncodeSQLRowBinaryAdaptive` API
and wire format are unchanged.

For empty input, `EncodeInto` returns a zero-length slice backed by the
caller-provided destination when possible. For non-empty input, callers should
retain the returned slice for the next call. Invalid rows and schemas preserve
the existing errors.

## Measurement

Workload: 128 rows with `INT64`, `DateTime`, and string columns, ten samples on
an AMD Ryzen 9 5950X. The paired benchmark runs the allocating and reusable
paths in the same benchmark fixture.

| Operation | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `EncodeSQLRowBinaryAdaptive` control | 29,670 | 17,784 | 20 | 1.00x |
| Reused `EncodeSQLRowBinaryAdaptiveInto` | 28,938 | 16,504 | 19 | 1.03x faster |

The independent pre-change baseline was 29,799 ns/op, 17,784 B/op, and 20
allocations. The improvement is intentionally opt-in: the codec still builds
all three candidates, while callers that retain a destination save the final
envelope allocation and about 7.2% of allocated bytes.

Benchmark command:

```text
make benchmark-row-binary-adaptive
make benchmark-row-binary-adaptive-into
```

## Verification

- A red test first failed because `EncodeSQLRowBinaryAdaptiveInto` was
  undefined.
- Focused normal and race tests pass.
- Full `hatSql` normal and race tests pass on the clean delivery base.
- Exact wire equality, destination backing reuse, empty input, and invalid-row
  errors are covered.
