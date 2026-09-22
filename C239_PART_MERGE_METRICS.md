# C239 Part-Merge Metrics

C239 adds `TypedTable.PartMergeMetrics()` for operational visibility into the
optional typed-table patch-part path. The zero-allocation snapshot reports:

- `pending_deletes`: logical tombstones waiting for physical compaction;
- `oldest_pending_age`: age of the oldest pending tombstone;
- `merge_count`: completed patch-part merges;
- `rows_read`, `rows_written`, and `deleted_rows`: cumulative physical row
  accounting across completed merges;
- `write_amplification`: cumulative `rows_written / deleted_rows`, or zero
  before a merge removes a row;
- `last_merge_at` and `last_merge_duration`.

The accounting is stored once per table, not once per tombstone. It does not
change the default physical-delete path, patch-part scheduling, SQL visibility,
or compaction threshold. `OldestPendingAge` is measured from the first pending
tombstone until the current snapshot; it resets when the backlog is compacted.

Example:

```go
metrics := table.PartMergeMetrics()
// metrics.PendingDeletes == 128
// metrics.OldestPendingAge == 42 * time.Second
// metrics.WriteAmplification == 6.5
```

`WriteAmplification` is row-based and cumulative. For example, rewriting 650
live rows to remove 100 tombstones reports `6.5`. It is a compaction-pressure
signal, not a byte-level storage ratio.

## Measurement

The pre-C239 baseline reconstructs the same counters by cloning the bounded
storage-event history. The C239 method reads direct counters under the table
read lock.

Linux/amd64, AMD Ryzen 9 5950X, `GOMAXPROCS=1`, five 2-second samples:

| Path | Samples (ns/op) | Median | Memory | Result |
| --- | --- | ---: | ---: | --- |
| C238 event-history reconstruction | 196.9, 228.6, 230.9, 216.4, 219.0 | 219.0 | 384 B/op, 1 alloc/op | baseline |
| C239 `PartMergeMetrics()` | 67.27, 69.05, 72.23, 65.05, 72.61 | 69.05 | 0 B/op, 0 allocs/op | 3.17x faster; allocation removed |

The benchmark measures status collection only. It does not claim that
compaction itself is faster; the change adds accounting to the existing
compaction path and leaves its scheduling behavior unchanged.

## Verification

```text
make test-c239-merge-metrics
make race-c239-merge-metrics
make vet-c239-merge-metrics
make benchmark-c239-merge-before-after
```
