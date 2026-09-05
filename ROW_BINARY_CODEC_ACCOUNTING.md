# RowBinary Codec Accounting

`hatSql.SQLRowBinaryCodecAccounting` is an opt-in meter for comparing RowBinary
codecs in a running process. It records logical bytes, encoded bytes, operation
counts, and the synchronous elapsed time of encode and decode callbacks.

```go
var meter hatSql.SQLRowBinaryCodecAccounting

encoded, err := meter.MeasureEncode(logicalBytes, func() ([]byte, error) {
	return hatSql.EncodeSQLRowBinary(rows, columns)
})
if err != nil {
	return err
}
if err := meter.MeasureDecode(encoded, logicalBytes, func(data []byte) error {
	_, err := hatSql.DecodeSQLRowBinary(data, columns)
	return err
}); err != nil {
	return err
}

report := meter.Snapshot()
```

`CompressionRatio` is `LogicalBytes / EncodedBytes`; a value above `1` means
the encoded representation is smaller. Successful operations contribute byte
totals. Failed callbacks still contribute operation and elapsed-time counters,
which makes codec errors visible without contaminating the ratio.

The duration is elapsed time around the synchronous callback, not a kernel CPU
time reading. It is a useful CPU-cost approximation for a CPU-bound codec, but
can include scheduling time when a goroutine is preempted. The counters use
atomics, and the benchmark records `0 B/op` and `0 allocs/op`; `time.Now` calls
still add about `141-143 ns` for one encode/decode pair on the reference host.

The meter is not installed on default encode/decode paths, so existing callers
pay no measurement cost and all existing wire formats remain unchanged. Use it
for sampled or diagnostic workloads, rather than wrapping every small request
when latency is the primary concern. `Reset` clears the accumulated counters.
