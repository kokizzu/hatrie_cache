# Per-Space WAL Sync Policy

`CommandJournal` keeps synchronous fsync-before-apply as the zero-value
default. This feature adds opt-in policies for logical spaces represented by
key prefixes. The command request does not carry a separate space field, so a
prefix is the explicit and auditable boundary.

## Configuration

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	GroupCommitMaxBatch: 1,
	SyncPolicy: hatCache.CommandJournalSyncPolicy{
		PeriodicInterval: 100 * time.Millisecond,
		Rules: []hatCache.CommandJournalSyncPolicyRule{
			{SpacePrefix: "cache:", Mode: hatCache.CommandJournalSyncPeriodic},
			{SpacePrefix: "volatile:", Mode: hatCache.CommandJournalSyncDisabled},
		},
	},
})
```

The longest matching prefix wins. An unmatched key is synchronous. A mixed
batch uses the strongest mode present, so one synchronous key keeps the whole
batch synchronous.

## Modes

| Mode | Apply behavior | Crash durability | Use when |
| --- | --- | --- | --- |
| `synchronous` | Append, sync, then apply | Every successful write | Durable data; default and recommended mode |
| `periodic` | Append and apply; a later write syncs after the interval, or call `Sync` | Up to one configured interval can be pending; graceful close flushes pending periodic data | High-rate data where bounded loss is acceptable |
| `disabled` | Append and apply without an automatic sync | The whole unsynced tail can be lost after a crash; graceful close does not force a sync | Rebuildable or explicitly volatile data |

`Sync()` always forces the current tail regardless of the configured mode:

```go
durableSequence, err := journal.Sync()
report := journal.Durability()
```

`Durability()` reports `AppliedSequence`, `DurableSequence`, `Pending`, the
last sync time, and the last sync error. Periodic mode is write-triggered and
does not start an idle goroutine. A caller that needs a time-based boundary
while idle should call `Sync` from its own scheduler.

The policy does not change journal encryption, framing, authentication, or
backup verification. It changes only when the journal file is synchronized;
operators must treat `periodic` and `disabled` as explicit durability
tradeoffs.

## Measurement

The benchmark uses repeated `SET` operations against one key and reports the
same allocations for every mode. The no-op sync-hook run isolates policy
overhead:

The pre-change baseline, measured before the policy implementation, was
`4,451 ns/op`, `262 B/op`, `2 allocs/op`, and `1.000 syncs/op`. The post-change
synchronous row below stays within normal benchmark noise while the policy
matcher adds no allocation.

| Mode | Time/op | Heap/op | Allocs/op | Syncs/op |
| --- | ---: | ---: | ---: | ---: |
| Synchronous baseline | 4,084 ns | 262 B | 2 | 1.000 |
| Periodic, one-hour window | 4,067 ns | 262 B | 2 | 0 |
| Disabled | 4,059 ns | 263 B | 2 | 0 |

The real-file `Sync` run measures the durability cost on the test host:

| Mode | Time/op | Heap/op | Allocs/op | Syncs/op |
| --- | ---: | ---: | ---: | ---: |
| Synchronous | 803,988 ns | 260 B | 2 | 1.000 |
| Periodic, one-hour window | 3,983 ns | 262 B | 2 | 0 |
| Disabled | 4,019 ns | 263 B | 2 | 0 |

Relative to synchronous real-file sync, periodic was `201.9x` faster and
disabled was `200.1x` faster in this run. The gain is primarily avoided fsync
latency, not a faster trie operation. Filesystem, device, and workload
latency will change the absolute numbers.

## Verification

Focused tests cover default synchronous behavior, longest-prefix validation,
periodic boundaries, explicit flush and replay, group commit, prepared
batches, mixed public batches, and durability reporting. The default path is
unchanged when `SyncPolicy` is omitted.
