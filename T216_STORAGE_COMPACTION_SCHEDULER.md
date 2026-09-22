# T216 Storage Compaction Scheduler

T216 adds caller-driven Vinyl-style compaction scheduling for importable
`hatDataStructure.StorageSpace` instances. It tracks physical disk usage per
space and compacts only on-disk spaces whose stale-record debt crosses an
explicit threshold.

## Default Behavior

There is no background goroutine and no automatic scheduler registration.
Existing `StorageSpace` behavior is unchanged until a caller constructs a
`StorageSpaceCompactionScheduler` and invokes `Run`.

The zero-valued scheduler options use conservative defaults:

- `MinStaleBytes`: 1 MiB
- `MinStaleRatio`: disabled
- `MaxSpacesPerRun`: 1

`memtx` spaces are considered and skipped without compaction. On-disk spaces
are selected in deterministic name order. A run is bounded by
`MaxSpacesPerRun`, honors context cancellation, and reports considered,
scheduled, completed, skipped, failed, and reclaimed-byte counts.

## Space Accounting

`StorageSpaceStats` reports:

- `DiskBytes`: all bytes currently in the append-only spill segment;
- `LiveDiskBytes`: bytes referenced by current cold values;
- `StaleDiskBytes`: obsolete bytes from replacements and deletes;
- `CompactionDebtBytes`: the current stale-byte debt, equal to
  `StaleDiskBytes` for this storage policy.

The counters are maintained during recovery, writes, flushes, deletes, and
compaction. `Stats` therefore remains an allocation-free constant-time read;
it does not scan the entry map. The counter is one additional `int64` per
spill arrangement, not per key or value.

## Example

```go
scheduler, err := hatDataStructure.NewStorageSpaceCompactionScheduler(
	 hatDataStructure.StorageSpaceCompactionSchedulerOptions{
		MinStaleBytes:   1 << 20,
		MinStaleRatio:   0.25,
		MaxSpacesPerRun: 2,
	},
)
if err != nil {
	return err
}
if err := scheduler.Register(coldSpace); err != nil {
	return err
}
run, err := scheduler.Run(ctx)
if err != nil {
	return err
}
log.Printf("compaction completed=%d reclaimed=%d", run.Completed, run.ReclaimedBytes)
```

Compaction is synchronous and caller-owned. Run it from an existing
maintenance loop with a deadline rather than creating an unbounded process
background worker. Call `Flush` and `Sync` before a backup or checkpoint when
the backup must include the latest hot values. Compaction rewrites the live
cold records and preserves logical entries, so a backup may be taken before or
after it according to the desired physical-byte tradeoff.

## Verification And Cost

Run the focused checks with:

```text
make test-t216
make race-t216
make vet-t216
make benchmark-t216
```

On Linux/amd64 with an AMD Ryzen 9 5950X, the 256-entry benchmark measured
the initial map-scan accounting implementation at `2.066 us/op` median,
`0 B/op`, and `0 allocs/op`. Maintaining the counter reduced the same stats
read to `30.21 ns/op`, `0 B/op`, and `0 allocs/op`, about `68.4x` faster. The
memtx stats control measured `16.18 ns/op`, so the disk-policy accounting is
about `1.87x` the control read while still remaining constant-time.

Compaction itself is explicit maintenance work and remains approximately
`1.6-1.9 ms/op`, `71,072 B/op`, and `531 allocs/op` for the benchmark fixture.
The optimization adds no per-entry allocation and does not change the
logical or backup format.
