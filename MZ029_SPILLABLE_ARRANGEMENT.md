# MZ-029 Spillable Arrangements

Persisted reopen index details and recovery guidance: [MZ029_PERSISTED_INDEX.md](MZ029_PERSISTED_INDEX.md).

`hatDataStructure.SpillableArrangement` is an opt-in local spill tier for
large keyed byte payloads. It keeps the key and point-lookup metadata in RAM,
retains recent value payloads up to a configured budget, and moves older
payloads to a private binary segment. This bounds the large value allocation
that can otherwise contribute to process-memory exhaustion.

It is a storage tier, not a durability or replication protocol. The segment is
created fresh for each arrangement, is CRC-protected, and is not reopened as a
restored arrangement. Call `Sync` or `Flush` when local filesystem persistence
of already-written spill records matters. Spilled values are plaintext; use an
encrypted filesystem or an application-level encryption layer for sensitive
data.

## Behavior

- `Set` clones caller values and replaces the previous value atomically for
  validation and configured disk-limit failures.
- `Get` returns a new byte slice for both hot and cold values.
- Hot value payload bytes are bounded by `MemoryLimitBytes`; keys, map entries,
  and spill references remain resident for O(1) point lookup.
- Automatic eviction is FIFO by write generation, which is deterministic and
  avoids a per-entry linked-list allocation.
- `Flush` spills all hot values and syncs the segment.
- `Delete` removes the live key immediately; stale segment bytes remain until
  `Compact`.
- `Compact` rewrites only live cold values into a fresh segment and atomically
  publishes it.
- `MaxDiskBytes` is an optional hard bound. Deletes and replacements can leave
  stale bytes, so compact before retrying a disk-limited workload.
- Segment records contain a magic header, bounded lengths, the key, value, and
  a CRC-32 checksum. Truncated, tampered, or mismatched records return
  `ErrSpillableArrangementCorrupt`.

## Example

```go
arrangement, err := hatDataStructure.NewSpillableArrangement(
	hatDataStructure.SpillableArrangementOptions{
		MemoryLimitBytes: 8 << 20,
		MaxDiskBytes:     8 << 30,
	},
)
if err != nil {
	return err
}
defer arrangement.Close()

if err := arrangement.Set("customer:42", payload); err != nil {
	return err
}
value, found, err := arrangement.Get("customer:42")
if err != nil || !found {
	return err
}
_ = value
```

For a caller-owned directory, set `Directory` to keep the segment available
for inspection and backup after `Close`. With an empty directory, the
arrangement creates and removes a private temporary directory on close.

Zero limits use sane defaults: 64 MiB hot value payload, 1 MiB maximum key,
and 64 MiB maximum value. `MaxDiskBytes == 0` means unlimited segment bytes;
set it explicitly when disk exhaustion must be rejected.

## Measured Tradeoff

The control and candidate both clone a 256-byte value on every read. The test
loads 4,096 keys, or 1,048,576 bytes of value payload. The candidate uses a
1-byte hot payload limit, so all values are cold and the retained value budget
is effectively zero while key/index metadata stays resident.

| Path | Median time | Median bytes | Median allocations | Result |
| --- | ---: | ---: | ---: | --- |
| In-memory copy-on-read baseline | 94.06 ns/op | 256 B/op | 1 alloc/op | 1.00x |
| Cold spill-segment read | 975.0 ns/op | 288 B/op | 1 alloc/op | 10.4x slower, 1.125x bytes |

Raw five-sample benchmark output:

```text
BenchmarkSpillableArrangementInMemoryBaseline-32
95.62 ns/op  256 B/op  1 allocs/op
96.71 ns/op  256 B/op  1 allocs/op
91.05 ns/op  256 B/op  1 allocs/op
91.12 ns/op  256 B/op  1 allocs/op
94.06 ns/op  256 B/op  1 allocs/op

BenchmarkSpillableArrangementColdGet-32
977.7 ns/op  288 B/op  1 allocs/op
1023 ns/op   288 B/op  1 allocs/op
973.1 ns/op  288 B/op  1 allocs/op
975.0 ns/op  288 B/op  1 allocs/op
959.5 ns/op  288 B/op  1 allocs/op
```

The feature is therefore useful when avoiding retained-memory failure is more
important than hot-read latency. Existing in-memory behavior is unchanged;
callers must explicitly construct this type to accept the I/O tradeoff.

## Verification

Tests cover hot/cold round trips, caller ownership, sorted snapshots, deletes,
stale-byte compaction, disk-limit atomicity, CRC corruption, invalid limits,
close behavior, private-directory cleanup, and the memory-bound accounting.

```text
make verify-mz029-spillable-arrangement
make benchmark-mz029-spillable-arrangement
```
