# TR-018 WAL Sync Modes

`hatJournal.Options` now exposes an explicit WAL sync policy for journaled
collections. The zero value is unchanged: synchronous `fsync` at every
completed collection.

## Modes

```go
options := hatCache.CommandJournalOptions{
    GroupCommitMaxBatch: 64,
    SyncMode:            hatCache.CommandJournalSyncModePeriodic,
    SyncInterval:        2 * time.Second,
}
journal, err := hatCache.OpenCommandJournalWithOptions(path, options)
```

| Mode | Collection behavior | Crash-loss contract |
| --- | --- | --- |
| `immediate` | `fsync` every completed collection | A successful collection has the existing synchronous durability boundary |
| `periodic` | First collection syncs, later collections sync when `SyncInterval` elapses | Accepted writes since the last sync can be lost after an OS/host crash; default interval is 1 second |
| `disabled` | No automatic `fsync` | The caller explicitly accepts loss of OS-buffered journal writes after a process/host crash |

The journal bytes are still appended and replayable in periodic and disabled
mode. Only the automatic sync boundary changes. A caller can force a durable
checkpoint with `journal.Sync()` regardless of the configured mode. A failed
automatic sync still fails the current append/collection and retains the
existing rollback behavior.

`hatCache.ParseCommandJournalSyncMode` accepts `immediate`/`sync`, `periodic`,
and `disabled`/`none`. Invalid modes and negative intervals fail closed during
`OpenCommandJournalWithOptions`. `SyncInterval` is ignored for immediate and
disabled modes; periodic mode uses `DefaultSyncInterval` when it is zero.

## Choosing a mode

- Use `immediate` for the default durability contract, financial or control
  state, and any journal that is the only recovery source.
- Use `periodic` when a bounded recovery point objective is acceptable and
  reducing sync frequency matters more than per-write durability.
- Use `disabled` only when another durable layer or explicit checkpoints cover
  the loss window. It is not a safe performance switch.

The policy is evaluated at collection boundaries by the owning journal; it
does not create a background sync goroutine or an unbounded timer queue.

## Measurement

`make tg18-benchmark-journal` measures the policy decision itself, excluding
filesystem `fsync` latency. Five samples on an AMD Ryzen 9 5950X produced:

| Policy check | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Immediate | 1.977 | 0 | 0 |
| Periodic | 8.058 | 0 | 0 |
| Disabled | 1.962 | 0 | 0 |

The default immediate path avoids a time read when deciding to sync. The
periodic check adds about 6.08 ns over the immediate policy decision; actual
latency is dominated by the selected filesystem sync cost. The feature is
therefore a durability/IO policy, not a claim that periodic mode is universally
faster for application throughput.
