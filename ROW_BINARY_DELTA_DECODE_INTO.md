# Reusable RowBinary Delta Decoder

`hatSql.DecodeSQLRowBinaryDeltaInto` decodes both HSD1 first-order and HSD2
second-order RowBinary delta streams into a caller-owned `[]SQLRow` buffer.
`DecodeSQLRowBinaryDelta` now delegates to the same implementation with a nil
destination, so the existing API and wire format remain unchanged.

Pass the returned slice back as `rows[:0]` on the next same-shape decode:

```go
rows, err := hatSql.DecodeSQLRowBinaryDeltaInto(nil, columns, wire)
if err != nil {
	return err
}
rows, err = hatSql.DecodeSQLRowBinaryDeltaInto(rows[:0], columns, nextWire)
```

The reusable path keeps row maps and compatible string, bytes, and JSON values
when their capacity or contents can be reused. It still validates column
metadata, headers, row limits, NULL markers, value bounds, and trailing bytes.
The destination belongs to the caller and must not be used concurrently by
multiple decoders. Passing a nil destination remains the allocating one-shot
mode.

## Verification

`make test-row-binary-delta-decode-into` checks exact round trips for both HSD1
and HSD2 and verifies row-slice reuse. `make test-row-binary-delta-into`
continues to cover the existing encoder paths.

## Measurement

The benchmark uses 4,096 rows with sequential `INT64`, `DateTime`, nullable
amount, and repeated string columns. It runs ten samples on an AMD Ryzen 9
5950X and reports CPU time, heap bytes, allocations, and wire size.

| Format | Pre-change decode | Warm `DecodeInto` | Speedup | Heap | Allocs | Wire |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HSD1 | 1,347,920 ns/op | 734,000 ns/op | 1.84x | 1,703,567 -> 226,653 B/op | 28,357 -> 16,037 | 77,518 B |
| HSD2 | 1,362,559 ns/op | 723,127 ns/op | 1.88x | 1,703,370 -> 226,653 B/op | 28,357 -> 16,037 | 57,052 B |

The HSD1 warm median was rounded to 734,000 ns/op from the captured
ten-sample run; the raw samples were `763046 773622 754817 768923 746880
713132 718286 721119 711636 717867`. The exact sorted median is 733,999.5
ns/op. HSD2 raw warm samples were
`704115 702177 710560 724821 715414 729100 721433 735696 742714 752608`.

The reusable path trades retained destination capacity for lower per-batch
allocation and CPU cost. It does not reduce bandwidth because HSD1/HSD2 bytes
are unchanged.
