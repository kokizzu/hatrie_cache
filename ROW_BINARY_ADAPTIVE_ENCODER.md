# Reusable Adaptive RowBinary Encoder

`hatSql.SQLRowBinaryAdaptiveEncoder` is an opt-in stateful encoder for callers
that repeatedly encode the same schema shape. Its zero value is ready to use:

```go
var encoder hatSql.SQLRowBinaryAdaptiveEncoder
var destination []byte

destination, err = encoder.EncodeInto(destination, columns, rows)
if err != nil {
	return err
}

// Reuse both buffers on the next call.
destination, err = encoder.EncodeInto(destination[:0], columns, nextRows)
```

The encoder preserves the existing adaptive selection between legacy,
first-order delta, and second-order delta RowBinary candidates. It reuses the
candidate payload buffers and per-column delta scratch between calls, then
copies only the selected HSA1 envelope into the caller-owned destination.
The returned wire bytes are equivalent to
`EncodeSQLRowBinaryAdaptiveInto` for the same input.

The zero value is usable, but one encoder must have one owner and must not be
used concurrently. It retains the largest candidate buffers seen so far, so a
one-shot call should continue using the stateless API. `Reset` releases the
retained buffers when a workload phase ends or a high-water input should no
longer be held. The existing allocating and stateless `Into` APIs are
unchanged.

## Measurement

Fixture: 128 rows with `INT64`, `DateTime`, and string columns; both paths
produced 1,181 wire bytes; ten samples on an AMD Ryzen 9 5950X.

| Operation | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Stateless `EncodeSQLRowBinaryAdaptiveInto` | 29,191 | 16,504 | 19 | 1.00x |
| Warm `SQLRowBinaryAdaptiveEncoder.EncodeInto` | 22,192 | 0 | 0 | 1.32x faster |

The `B/op` value for the reusable path is per warm call and does not include
the encoder's retained high-water buffers. The retained memory is the explicit
tradeoff for eliminating repeated candidate allocations; call `Reset` when
that retained capacity is no longer useful.

Correctness coverage includes wire-equivalence, destination reuse, reset,
invalid input recovery, normal execution, and the race-enabled focused test.
