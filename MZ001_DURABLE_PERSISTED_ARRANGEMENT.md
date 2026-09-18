# MZ-001 Durable Persisted Spillable Arrangement

This is a partial adoption of Materialize's durable persisted-collection idea
for `hatDataStructure.SpillableArrangement`. A flushed spill segment can be
reopened after a process restart without rebuilding the arrangement from the
source rows.

## Usage

```go
options := hatDataStructure.SpillableArrangementOptions{
	Directory:        "/var/lib/hatrie/arrangements",
	MemoryLimitBytes: 64 << 20,
	MaxDiskBytes:     8 << 30,
}

arrangement, err := hatDataStructure.NewSpillableArrangement(options)
if err != nil {
	return err
}
if err := arrangement.Set("region:sg", []byte("ready")); err != nil {
	return err
}
if err := arrangement.Flush(); err != nil {
	return err
}
path := arrangement.SpillPath()
if err := arrangement.Close(); err != nil {
	return err
}

arrangement, err = hatDataStructure.OpenSpillableArrangement(path, options)
```

`Flush` is the durability boundary. It writes cold records and calls
`File.Sync`; `SpillPath` identifies the segment that can be copied by backup
 tooling after the arrangement is quiescent. `OpenSpillableArrangement`
validates every record's magic, lengths, reserved header bytes, and CRC before
publishing any recovered entry.

Recovery retains the latest key and file offset for each key. Values stay cold
on disk until `Get` or `Snapshot`, so reopening does not retain the full value
payload in heap memory. Replacements are resolved by latest-record-wins
replay, and the normal `Set`, `Flush`, `Compact`, and `Close` methods remain
available after reopening.

## Safety And Scope

- Empty segments are valid; truncated records, invalid lengths, bad checksums,
  non-regular paths, and symlink paths are rejected.
- `MaxKeyBytes`, `MaxValueBytes`, and `MaxDiskBytes` are enforced during
  recovery before large buffers are allocated.
- The segment is a local durable file, not a replicated blob store. This
  feature does not add consensus handles, distributed garbage collection,
  manifest publication, or automatic backup coordination.
- Call `Flush` before backup and avoid copying a segment while another process
  is writing it. Use the existing backup/restore coordination for a consistent
  application-level snapshot.
- The feature is opt-in. `NewSpillableArrangement` still creates a new segment
  and existing in-memory behavior is unchanged.

## Measurement

The benchmark recovers 256 keys with 13-byte values on Linux/amd64 using five
`-benchmem` samples on an AMD Ryzen 9 5950X. The baseline rebuilds a fresh
arrangement from the already decoded rows; the durable path scans the flushed
segment and retains only key/offset metadata.

| Recovery path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Rebuild from snapshot rows | 338,574 | 68,757 | 561 | 1.00x |
| Open durable segment | 78,797 | 75,288 | 538 | 4.30x faster; 1.09x bytes; 0.96x allocations |

The durable path is about 4.3x faster and allocates 4.1% fewer objects, with a
9.5% higher transient allocation volume for this small workload. Reopened
`Stats` report zero hot value bytes; the extra benchmark bytes are temporary
scan and key-index allocations, not retained value payload. The benchmark is
run with:

```text
make benchmark-mz01-durable-arrangement
```

Raw samples:

```text
BenchmarkMZ01RebuildFromSnapshot-32    5932  173958 ns/op  68734 B/op  561 allocs/op
BenchmarkMZ01RebuildFromSnapshot-32    7425  198999 ns/op  68730 B/op  561 allocs/op
BenchmarkMZ01RebuildFromSnapshot-32    3014  339197 ns/op  68816 B/op  561 allocs/op
BenchmarkMZ01RebuildFromSnapshot-32    3078  338574 ns/op  68757 B/op  561 allocs/op
BenchmarkMZ01RebuildFromSnapshot-32    3672  334754 ns/op  68786 B/op  561 allocs/op
BenchmarkMZ01OpenDurableSegment-32    14986   78070 ns/op  75288 B/op  538 allocs/op
BenchmarkMZ01OpenDurableSegment-32    15360   79329 ns/op  75288 B/op  538 allocs/op
BenchmarkMZ01OpenDurableSegment-32    14368   79870 ns/op  75288 B/op  538 allocs/op
BenchmarkMZ01OpenDurableSegment-32    15111   78797 ns/op  75288 B/op  538 allocs/op
BenchmarkMZ01OpenDurableSegment-32    15225   78443 ns/op  75256 B/op  538 allocs/op
```
