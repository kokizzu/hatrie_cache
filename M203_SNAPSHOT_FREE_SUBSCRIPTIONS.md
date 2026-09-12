# M203: Snapshot-Free Journal Subscriptions

`CommandJournalSubscribeOptions.SkipReplay` starts a subscription at the current journal sequence without replaying historical records. It is intended for consumers that already have the current cache state and only need subsequent changes.

```go
subscription, err := journal.Subscribe(ctx, hatCache.CommandJournalSubscribeOptions{
	SkipReplay:   true,
	Buffer:       256,
	PollInterval: time.Second,
})
```

The subscription is registered before its current sequence is captured under the journal lock. Records appended before that capture are part of the current state and are skipped; records appended after the capture are delivered. This avoids a replay/setup gap without changing the existing replay path. `AfterSequence` is ignored when `SkipReplay` is true.

`SkipReplay` defaults to `false`, so existing callers retain their current behavior. A caller must only use it when its local state is already current at subscription time; it is not a replacement for replay-based recovery after a disconnected consumer.

## Benchmark

Measured on an AMD Ryzen 9 5950X, Linux amd64, Go, five samples per case, `-benchtime=1s -benchmem`. The fixture contains 100 existing journal records.

| Mode | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Existing replay of 100 records | 92,451 | 73,941 | 520 |
| `SkipReplay` | 2,306 | 1,368 | 11 |
| Replay / snapshot-free | 40.1x | 54.1x | 47.3x |

Run the benchmark with:

```text
make benchmark-m203
```

## Verification

```text
make verify-m203
```

The focused regression test proves historical records are omitted and the next append is delivered with its original journal sequence.
