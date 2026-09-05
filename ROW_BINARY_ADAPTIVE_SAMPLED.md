# Sampled Adaptive RowBinary

`EncodeSQLRowBinaryAdaptiveSampled` is an opt-in CPU-first alternative to
`EncodeSQLRowBinaryAdaptive`.

```go
encoded, err := hatSql.EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 32)
if err != nil {
	return err
}
decoded, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, encoded)
```

For a non-empty batch, `sampleRows` must be positive. A value larger than the
batch is clamped. The encoder tests legacy, delta, and double-delta payloads on
the prefix, then encodes the complete batch once with the selected codec. It
uses the existing `HSA1` envelope, so `DecodeSQLRowBinaryAdaptive` needs no
change and all existing adaptive payloads remain valid.

The tradeoff is intentional. On the reference host with 1,024 stationary rows
and integer, timestamp, and repeated-string columns, full adaptive encoding was
about `204-207 us`, `209,400 B/op`, and `32 allocs/op`; sampled selection with a
32-row prefix was about `71 us`, `42,360 B/op`, and `21 allocs/op`. Both emitted
`9,245` wire bytes. The speedup comes from replacing three full candidate
encodes with three small sample encodes and one full encode.

Sampling can choose badly when the batch changes shape. With one integer column
whose first 32 rows were sequential and whose remaining rows were wide-range
jumps, full adaptive used about `77-83 us` and emitted `2,821` bytes, while the
sampled path used about `34-35 us` but emitted `8,973` bytes. Legacy emitted
`8,192` bytes. In that case sampled output was about `3.18x` larger than the
full-adaptive output and `1.10x` larger than legacy, despite being faster.

Use sampled selection when encode CPU is more important than maximum compression
and batches are known to be reasonably stationary. Keep full adaptive for
bandwidth-first workloads. Neither path is enabled by default, and the legacy
RowBinary format is unchanged.
