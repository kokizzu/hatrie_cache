# Reusable RowBinary Delta Encoding

The explicit HSD1 and HSD2 delta formats are useful for ordered counters and
timestamps. `EncodeSQLRowBinaryDeltaInto` and
`EncodeSQLRowBinaryDoubleDeltaInto` reuse a caller-owned destination without
changing the delta wire format:

```go
wire, err := hatSql.EncodeSQLRowBinaryDeltaInto(nil, columns, rows)
if err != nil {
	return err
}
for _, nextBatch := range batches {
	wire, err = hatSql.EncodeSQLRowBinaryDeltaInto(wire[:0], columns, nextBatch)
	if err != nil {
		return err
	}
	send(wire)
}
```

Use `EncodeSQLRowBinaryDoubleDeltaInto` for the HSD2 second-order format. The
returned bytes may alias the destination and are invalidated when it is reused.
The existing allocating functions remain available and delegate to the same
validated implementation. Adaptive HSA1 selection and its retained candidate
state are unchanged; these are explicit opt-in APIs.

## Measurement

The 4,096-row workload used sequential `INT64`, `DateTime`, nullable amount,
and repeated string columns. Ten samples on an AMD Ryzen 9 5950X produced:

| Operation | Median ns/op | B/op | Allocs/op | Wire bytes | Relative latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing HSD1 encoder | 385,192 | 337,739 | 16 | 77,518 | 1.00x |
| Warm HSD1 `EncodeInto` | 302,599 | 618 | 10 | 77,518 | 1.27x faster |
| Existing HSD2 encoder | 345,887 | 165,654 | 13-14 | 57,052 | 1.00x |
| Warm HSD2 `EncodeInto` | 288,806 | 541 | 10 | 57,052 | 1.20x faster |

The improvement is buffer reuse only; bandwidth and codec selection are
unchanged.
