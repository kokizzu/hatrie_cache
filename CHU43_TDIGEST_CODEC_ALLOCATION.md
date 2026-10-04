# CH-U43 T-Digest Codec Allocation Optimization

`TDigest.MarshalAggregateState` previously called `Snapshot`, which copied the
centroid slice before encoding. The encoder only reads the digest, so it now
validates a read-only view of the existing slice and allocates only the wire
buffer.

## Benchmark

Command:

```text
go test ./hat/hatDataStructure -run '^$' -bench 'TDigestAggregateStateCodec' -benchmem -count=5
```

Measured on AMD Ryzen 9 5950X, Linux amd64. Values are the mean of five runs.

| Operation | Size | Before ns/op | After ns/op | CPU improvement | Before B/op | After B/op | Allocation change | Wire bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Marshal compact | 4096 | 3930 | 3425 | 1.15x faster | 10752 | 5376 | 2 -> 1 | unchanged |
| Marshal compact | 16384 | 9535 | 8613 | 1.11x faster | 27136 | 13568 | 2 -> 1 | unchanged |

The JSON and unmarshal paths were not changed. The compact wire format remains
compatible because only the temporary in-memory snapshot copy was removed.

## Verification

- Focused allocation regression test passes.
- Full `hat/hatDataStructure` tests pass.
- Race-enabled `hat/hatDataStructure` tests pass.
- `go vet ./hat/hatDataStructure` is run before commit.
