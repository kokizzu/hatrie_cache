# CH-U43 T-Digest Compact Unmarshal Optimization

`NewTDigestFromAggregateState` already decodes centroids into a newly allocated
slice. It previously passed that slice to `NewTDigestFromSnapshot`, which
validated it and copied it again. The decoder now validates the slice and takes
ownership of that newly allocated memory directly.

## Benchmark

Command:

```text
go test ./hat/hatDataStructure -run '^$' -bench 'TDigestAggregateStateCodec/UnmarshalCompact' -benchmem -count=5
```

Measured on AMD Ryzen 9 5950X, Linux amd64. Values are the mean of five runs.

| Operation | Before ns/op | After ns/op | CPU improvement | Before B/op | After B/op | Allocation change | Wire bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| Unmarshal compact, 4096 | 3186 | 2642 | 1.21x faster | 16136 | 10760 | 4 -> 3 | unchanged |
| Unmarshal compact, 16384 | 7619 | 5539 | 1.38x faster | 40712 | 27144 | 4 -> 3 | unchanged |

The decoder still owns its centroid slice, so mutating the caller's input bytes
cannot mutate the returned digest. The wire format and validation errors are
unchanged.

## Verification

- Focused allocation regression test passes.
- Full `hat/hatDataStructure` tests pass.
- Race-enabled `hat/hatDataStructure` tests pass.
- `go vet ./hat/hatDataStructure` passes.
