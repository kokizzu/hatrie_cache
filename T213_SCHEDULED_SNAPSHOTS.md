# T213 Scheduled Snapshots

T213 adds an opt-in scheduler around the existing resumable snapshot exporter.
It combines three ideas from the inspiration backlog:

- periodic snapshot production instead of caller-managed timing;
- a durable checkpoint manifest containing the published snapshot digest and
  journal coordinate;
- atomic publication so readers see either the previous complete snapshot or
  the new complete snapshot.

## API

```go
scheduler, err := hatCache.NewScheduledSnapshotter(journal, trie,
    hatCache.ScheduledSnapshotOptions{
        Interval:    15 * time.Minute,
        Destination: "/var/lib/hatrie/latest.snapshot",
        Format:      hatCache.SnapshotFormatBinary,
    })
if err != nil {
    return err
}
if err := scheduler.Start(); err != nil {
    return err
}
defer scheduler.Stop()
```

`RunNow` performs one serialized export without starting the background ticker.
`RunImmediately` requests one export when `Start` begins. `OnPublished` receives
only after both the snapshot and its manifest have been atomically committed;
`OnError` receives background-run failures.

## Files and recovery

For destination `latest.snapshot`, the defaults are:

- `latest.snapshot.resume.json`: resumable copy checkpoint. A failed export
  leaves it in place and the next run resumes from the verified prefix.
- `latest.snapshot.manifest.json`: durable `ScheduledSnapshotCheckpoint`,
  written with the existing fsync, rename, and directory-sync helper after the
  snapshot target is published.

`ManifestPath` and `CheckpointPath` can override those locations. The default
snapshot and journal paths are unchanged because the scheduler is never
created unless a caller opts in. `ReadScheduledSnapshotCheckpoint` validates
the version, digest, format, timestamp, and regular-file boundary before a
checkpoint is accepted.

`Stop` waits for an in-flight snapshot to finish. Snapshot runs are serialized,
so a slow export cannot overlap a second export or publish a stale manifest.

## Measurement

Five `-benchmem` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X over the same 128-key binary snapshot workload.

| Path | Raw ns/op samples | Median ns/op | Memory/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing resumable export | `8258581 7306868 7239142` | `7306868` | `251466 B` | `1059` | `1.00x` |
| Scheduled export + durable manifest | `9155332 9133899 9217752` | `9155332` | `256769 B` | `1081` | `1.25x` |

The opt-in manifest publication adds `1.85 ms/op` or about 25.3% CPU, 5,303
bytes/op or 2.1% memory, and 22 allocations/op. This is the expected cost of
the durable JSON manifest and fsync/rename boundary; the default snapshot path
does not call the scheduler and has no regression.

Commands:

```text
make test-t213
make benchmark-t213-baseline
make benchmark-t213
make race-t213
make vet-t213
```
