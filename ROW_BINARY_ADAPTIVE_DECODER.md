# Reusable Adaptive RowBinary Decoder

`DecodeSQLRowBinaryAdaptiveInto` and
`SQLRowBinaryAdaptiveDecoder.DecodeInto` are opt-in receive-side APIs for
repeated HSA1 RowBinary batches. They preserve the existing adaptive envelope
and all three payload codecs.

```go
var decoder hatSql.SQLRowBinaryAdaptiveDecoder
var rows []hatSql.SQLRow

rows, err = decoder.DecodeInto(rows, columns, encoded)
if err != nil {
	return err
}

// Reuse the row slice and its maps on the next batch.
rows, err = decoder.DecodeInto(rows[:0], columns, nextEncoded)
```

The first call creates the row slice and maps. Subsequent calls reuse those
maps, prune extra caller-added fields, and reuse byte buffers when possible.
The stateful decoder also retains the bounded per-column delta scratch; call
`Reset` to release that scratch. The caller owns the destination and must not
use one decoder concurrently from multiple goroutines. The existing
`DecodeSQLRowBinaryAdaptive` API and all default behavior remain unchanged.

## Measurement

Fixture: 128 rows with `INT64`, `DateTime`, and string columns; ten samples on
an AMD Ryzen 9 5950X. The benchmark compares the existing allocating decoder
with a warm reusable destination and decoder.

| Operation | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `DecodeSQLRowBinaryAdaptive` | 39,395 | 50,304 | 641 | 1.00x |
| Warm `DecodeSQLRowBinaryAdaptiveInto` | 17,689 | 5,171 | 259 | 2.23x faster |
| Warm `SQLRowBinaryAdaptiveDecoder.DecodeInto` | 17,685 | 5,120 | 256 | 2.23x faster |

The reusable path retains caller-owned row maps and values, so its per-call
allocation measurement excludes that retained destination memory. It still
allocates for boxed scalar/time values required by the generic `SQLRow` map;
the API is a large reduction, not a claim of zero allocations.

Correctness coverage includes legacy, delta, and double-delta envelopes;
nullable, scalar, bytes, JSON, date/time, duration, and UUID values; row-map
reuse; stale-field clearing; malformed-input recovery; normal execution; and
race-enabled focused tests.
