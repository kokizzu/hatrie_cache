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

The decoder path is unchanged. Its repeated-batch median moved from 84,902 to
84,276 ns/op in the paired runs, within normal benchmark noise, with the same
110,964 B/op and 1,794 allocations/op.
