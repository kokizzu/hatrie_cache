# MZ-029 Persisted Reopen Index

The spillable arrangement now writes an advisory sidecar index next to the
binary spill segment when `Flush` or `Sync` succeeds:

```text
<spill-segment>.idx
```

The sidecar stores the latest key-to-record references, record counts, the
segment size, and a checksum. Keys are sorted in the sidecar so the file is
deterministic and compact. It does not contain independent values; values
remain in the spill segment and are read lazily by `Get`.

## Recovery behavior

`OpenSpillableArrangement` validates the sidecar before using it:

- the sidecar must be a regular, non-symlink file within the bounded index
  size;
- the format version, checksum, segment size, record bounds, key lengths, and
  duplicate keys are checked;
- the referenced segment records are still validated when values are read.

If the sidecar is missing, stale, truncated, corrupt, or fails validation, the
arrangement falls back to the existing segment scan. The sidecar is therefore
an optimization, not a second source of truth. A failed sidecar write does
not make a successful segment flush fail.

`SpillIndexPath` returns the sidecar path for backup tooling. Back up the
spill segment as the authoritative data and include the `.idx` file when
available to reduce restore startup time. Restoring only the segment is valid;
the index will be rebuilt by the fallback scan. Restore both files atomically
from the same snapshot when possible so the sidecar matches the segment.

After mutations, call `Flush` or `Sync` before taking a backup. A flushed
delete is represented in the rebuilt index and remains deleted after reopen.
Compaction invalidates the old sidecar and writes a new one for the compacted
segment.

## Loader memory strategy

Small and typical sidecars use a pooled contiguous read buffer, avoiding a new
large allocation on every reopen. Larger sidecars use a streaming parser to
bound transient memory. Both paths apply the same validation rules.

## Benchmark

Workload: 4,096 spilled entries, 1-byte memory limit, repeated close/open and
one `Get`, `go test -benchmem -count=5`, AMD Ryzen 9 5950X, Linux amd64.

| implementation | median reopen | heap bytes/op | allocs/op | change vs scan |
| --- | ---: | ---: | ---: | ---: |
| segment scan (baseline) | 1,122,162 ns | 822,813 B | 8,252 | 1.00x |
| persisted index, pooled read | 545,588 ns | 582,426 B | 8,225 | 2.06x faster; 29.2% less heap |

Raw post-change samples:

```text
557126 ns/op  582426 B/op  8225 allocs/op
569521 ns/op  582322 B/op  8225 allocs/op
531796 ns/op  582352 B/op  8225 allocs/op
543548 ns/op  582445 B/op  8225 allocs/op
545588 ns/op  582644 B/op  8225 allocs/op
```

The earlier full-buffer prototype was faster but retained about 10% more heap
than the scan baseline, so it was replaced with the pooled/streaming design.
