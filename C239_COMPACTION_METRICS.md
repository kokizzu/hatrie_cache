# C239 Compaction Metrics Snapshot

This adopts a ClickHouse-style operational view for merge maintenance without
introducing a background worker or changing existing defaults.

`hatStorage.SnapshotCompactionMetrics` combines, in one bounded read:

- pending and running compaction tasks;
- oldest pending and running ages, calculated against a caller-supplied clock;
- scheduler completion, failure, and I/O-throttle counters;
- cumulative compaction input and output bytes;
- the latest reported debt gauge for every registered arrangement; and
- a precise input-to-output amplification ratio when output bytes are nonzero.

The diagnostics registry keeps cumulative byte counters even when its bounded
history has already rolled over. `CompactionDiagnostics.Summary` exposes those
aggregate counters without allocating or copying per-arrangement history.
Counters saturate at `uint64` maximum. `CurrentCompactionDebtBytes` is the sum
of each arrangement's latest debt observation, not a historical sum.

```go
metrics := hatStorage.SnapshotCompactionMetrics(scheduler, diagnostics, time.Now())
if ratio, ok := metrics.InputToOutputRatio(); ok {
	log.Printf("pending=%d age=%s amplification=%.2fx", metrics.Pending, metrics.OldestPendingAge, ratio)
}
```

The API is opt-in at the call site. It does not schedule work, read the wall
clock internally, change merge ordering, or alter the existing `Stats`, `Ages`,
or `Snapshot` contracts. A nil scheduler or diagnostics registry contributes
zero values.

## Cost And Measurement

Run:

```sh
make benchmark-ch239-compaction-metrics
```

The five samples below were collected on Linux/amd64 with an AMD Ryzen 9 5950X
using 64 queued tasks and four registered arrangements. The legacy control
materializes `Stats`, `Ages`, and `CompactionDiagnostics.Snapshot`, including
the bounded histories. The allocation-free control uses `Summary` instead;
the final path is `SnapshotCompactionMetrics`.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Legacy history materialization | 850.3, 845.6, 706.4, 1034, 818.3 | 845.6 | 832 | 6 |
| Existing allocation-free composition | 113.8, 105.9, 108.0, 105.4, 131.3 | 108.0 | 0 | 0 |
| Unified snapshot | 100.2, 101.3, 102.6, 98.95, 99.34 | 100.2 | 0 | 0 |

The unified read is `8.44x` faster than the legacy path, removes 832 B/op and
six allocations, and is `1.08x` faster than the already allocation-free
composition. The legacy path cannot recover cumulative bytes after history
rollover, so this comparison measures operational scrape cost rather than
claiming identical byte semantics. The tradeoff is three additional `uint64`
counters per registered arrangement, 24 bytes before struct alignment. The
aggregate call still scans the bounded arrangement map, so it should be used
as an explicit monitoring scrape rather than inserted into every foreground
operation.

Focused correctness coverage:

```sh
make test-ch239-compaction-metrics
```
