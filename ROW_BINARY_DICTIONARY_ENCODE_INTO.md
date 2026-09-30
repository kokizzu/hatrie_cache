# Reusable RowBinary Dictionary Encoding

`SQLRowBinaryDictionaryEncoder` is the stateful HDB1 transfer path for
low-cardinality string, bytes, and JSON columns. It retains dictionary values
between batches so repeated values travel as compact IDs, similar to
ClickHouse `LowCardinality` columns and Tarantool tuple transfer.

For repeated batches, use `EncodeInto`:

```go
encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload"})
if err != nil {
	return err
}

wire, err := encoder.EncodeInto(nil, rows)
if err != nil {
	return err
}

for nextBatch := range batches {
	wire, err = encoder.EncodeInto(wire[:0], nextBatch)
	if err != nil {
		return err
	}
	send(wire)
}
```

`EncodeInto` reuses the caller's destination when it has enough capacity and
retains the pending-dictionary and row-payload scratch buffers. Reusing
`wire[:0]` invalidates the previous contents, so a caller must finish sending
or copy a batch before the next call. The encoder remains single-owner and is
not safe for concurrent use. `Encode` remains available and delegates to the
same implementation with an independent output buffer.

The HDB1 wire format, dictionary selection, NULL behavior, dictionary growth
limits, failed-call atomicity, and `Reset` semantics are unchanged. The
optimization is allocation-focused and does not change bandwidth.

## Measurement

The existing 256-row repeated dictionary workload emitted 3,081 bytes in all
cases. Ten samples on an AMD Ryzen 9 5950X produced these medians:

| Operation | ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `Encode` before | 39,704 | 15,932 | 14 | 1.00x |
| Existing `Encode` after scratch reuse | 37,009 | 3,204 | 1 | 1.07x faster |
| `EncodeInto` with warm destination | 35,150 | 3-4 | 0 | 1.13x faster |

Before the decoder reuse addition, its repeated-batch median was 84,902 ns/op
with 110,964 B/op and 1,794 allocations/op; that path is benchmarked below.

## Reusable dictionary decoding

Use `DecodeInto` when the same schema and decoder process repeated batches:

```go
decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload"})
if err != nil {
	return err
}

decoded, err := decoder.DecodeInto(nil, firstWire)
if err != nil {
	return err
}
for _, wire := range laterWires {
	decoded, err = decoder.DecodeInto(decoded[:0], wire)
	if err != nil {
		return err
	}
	consume(decoded)
}
```

`DecodeInto` retains the caller's row maps, pending dictionary additions, and
bytes/JSON value backing buffers. The first batch must still be decoded so its
dictionary additions are installed. `Decode` remains the allocating wrapper;
the decoder is single-owner and `Reset` releases retained logical state while
keeping capacity available for reuse.

On the same ten-sample 256-row workload, warm `DecodeInto` measured 46,214
ns/op, 16,393 B/op, and 768 allocations versus 93,216 ns/op, 110,966 B/op,
and 1,794 allocations for the pre-change allocating path: 2.02x faster, 6.77x
less allocated heap, and 2.34x fewer allocations. HDB1 bytes remain unchanged.
