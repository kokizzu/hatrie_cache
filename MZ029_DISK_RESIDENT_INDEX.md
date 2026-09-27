# MZ029 Disk-Resident Arrangement Index

This extends the Materialize-inspired spillable arrangement with an explicit,
low-memory reopen mode. `SpillableArrangementOptions.DiskResidentIndex` keeps
the sorted `.idx` sidecar on disk and retains only sparse record offsets in
memory. Point lookups use bounded binary-search plus local record scans; value
bytes remain in the spill segment and are still CRC-checked on read.

The default is unchanged:

```go
options := hatDataStructure.SpillableArrangementOptions{
	Directory:        "/var/lib/hatrie/arrangements",
	MemoryLimitBytes: 64 << 20,
}
```

Enable the mode for large, mostly-read arrangements where restart memory and
reopen time matter more than repeated point-read latency:

```go
options.DiskResidentIndex = true
arrangement, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
if err != nil {
	return err
}
defer arrangement.Close()

value, found, err := arrangement.Get("customer:42")
```

## Mutation And Recovery

- A valid sidecar is validated for format, segment size, sorted unique keys,
  bounded references, and CRC before disk-resident mode is enabled.
- A missing, stale, corrupt, symlinked, or oversized sidecar falls back to the
  existing full in-memory recovery scan. The sidecar is never the source of
  truth for record bytes.
- `Set`, `Delete`, and `Compact` first materialize the normal in-memory index,
  then use the existing mutation path. This makes the mode safe for mixed
  read/write use without keeping two mutable indexes.
- `Snapshot` streams the sorted sidecar references into the requested output;
  its result is necessarily proportional to the requested snapshot.
- CRC validation of the segment record still happens on every cold value read.
- The mode is opt-in and does not change the normal constructor or default
  reopen behavior.

## Measured Tradeoff

Workload: 4,096 entries, 256-byte values, 1-byte hot-value limit, one lookup
of `key-3072`, Linux amd64, AMD Ryzen 9 5950X. Results are medians of five
`go test -benchmem` samples from:
`make benchmark-mz029-persisted-index`.

| Path | Median time | Median bytes/op | Median allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Map-resident reopen + one `Get` | 482,374 ns | 582,816 | 8,225 | baseline |
| Disk-resident reopen + one `Get` | 294,211 ns | 18,944 | 30 | 1.64x faster, 30.8x fewer bytes, 274.2x fewer allocations |
| Map-resident repeated `Get` | 937.2 ns | 288 | 1 | hot-read baseline |
| Disk-resident repeated `Get` | 14,110 ns | 336 | 7 | 15.1x slower, 1.17x bytes, 7x allocations |

Raw samples:

```text
BenchmarkMZ029DiskResidentReopenSpillableArrangement-32
294211 ns/op  18944 B/op  30 allocs/op
306079 ns/op  18944 B/op  30 allocs/op
294779 ns/op  18944 B/op  30 allocs/op
280907 ns/op  18944 B/op  30 allocs/op
281169 ns/op  18944 B/op  30 allocs/op

BenchmarkMZ029MapResidentGet-32
968.7 ns/op  288 B/op  1 allocs/op
935.7 ns/op  288 B/op  1 allocs/op
937.2 ns/op  288 B/op  1 allocs/op
938.5 ns/op  288 B/op  1 allocs/op
915.3 ns/op  288 B/op  1 allocs/op

BenchmarkMZ029DiskResidentGet-32
14027 ns/op  336 B/op  7 allocs/op
14730 ns/op  336 B/op  7 allocs/op
14607 ns/op  336 B/op  7 allocs/op
13955 ns/op  336 B/op  7 allocs/op
14110 ns/op  336 B/op  7 allocs/op

BenchmarkMZ029ReopenSpillableArrangement-32
489428 ns/op  582816 B/op  8225 allocs/op
482374 ns/op  582598 B/op  8225 allocs/op
493487 ns/op  582832 B/op  8225 allocs/op
481594 ns/op  582954 B/op  8225 allocs/op
472287 ns/op  582357 B/op  8225 allocs/op
```

Use this mode for memory-constrained restart/read-mostly workloads. Use the
default map-resident mode for hot point-read workloads; making the mode the
default would trade a large reopen-memory win for a roughly 15x point-read
regression.

## Verification

```text
make test-mz029-persisted-index
make race-mz029-persisted-index
make benchmark-mz029-persisted-index
```
