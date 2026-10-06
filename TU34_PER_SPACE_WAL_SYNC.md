# T-U34 Per-Space WAL Sync Policy

Hatrie Cache keeps its existing synchronous command-journal behavior by
default. This feature is opt-in through an explicit `CommandJournalSpace`
handle, so a space is never guessed from an arbitrary cache key.

```go
journal, err := hatCache.OpenCommandJournal("data/commands.journal")
if err != nil {
	return err
}
defer journal.Close()

orders, err := journal.OpenSpace("orders", hatCache.CommandJournalSpaceSyncPolicy{
	Mode:     hatCache.CommandJournalSpaceSyncModePeriodic,
	Interval: time.Second,
})
if err != nil {
	return err
}

response := orders.ExecuteCommand(trie, hatCache.CacheCommandRequest{
	Command: "SETSTR",
	Key:     "order:123",
	Value:   "paid",
})
if !response.OK {
	return errors.New(response.Message)
}
if err := orders.Flush(); err != nil {
	return err
}
```

## Modes

| Mode | WAL append | Apply/ack timing | Recovery tradeoff |
| --- | --- | --- | --- |
| `synchronous` | Yes | Sync before apply and return; existing group commit remains available | Strongest durability; default behavior and allocation profile are unchanged |
| `periodic` | Yes | Apply and return immediately; one lazy journal flush loop syncs pending bytes at the shortest configured interval | An acknowledged write can be lost during the interval or an OS crash; `Flush` and journal close force a final sync |
| `disabled` | No | Apply immediately while serialized with journaled writers | The command journal cannot recover the mutation; use a snapshot or another durable layer |

The empty policy mode normalizes to `synchronous`. Periodic mode requires a
positive interval. A failed periodic sync blocks later periodic writes until a
later background retry or an explicit successful `Flush`, and the error is
returned to the caller. A sync flush covers all pending bytes in the single
physical journal, regardless of which named space caused it.

`SetSpaceSyncPolicy` changes a named space without changing unregistered or
ordinary `CommandJournal` callers. The public HTTP/gRPC command paths still
use the existing journal unless an application explicitly routes a mutation
through a space handle.

## Safety

- Space names are explicit and validated; key prefixes are never interpreted
  as spaces.
- Existing `ExecuteCommand` and `OpenCommandJournal` defaults are unchanged.
- Periodic pending bytes are flushed during `Close`; close reports a sync
  failure instead of hiding it.
- Disabled mode is intentionally explicit and should not be used for data
  that must be restored from the command journal.

## Benchmark

The benchmark uses one `SETSTR` mutation, `GroupCommitMaxBatch=1`, and a
no-op sync hook to isolate policy and locking overhead from device latency.
Five samples were run with `-benchtime=250ms`; values below are medians.

| Case | ns/op | B/op | allocs/op | Relative to clean legacy |
| --- | ---: | ---: | ---: | ---: |
| Clean `f42ec7a5` legacy baseline | 4,222 | 256 | 2 | 1.00x |
| Current legacy control | 4,218 | 256 | 2 | 1.00x |
| Explicit synchronous space | 4,284 | 256 | 2 | 0.99x, 1.5% slower |
| Explicit periodic space | 4,592 | 260 | 2 | 0.92x, 8.8% slower before flush |
| Explicit disabled space | 320 | 0 | 0 | 13.19x faster |

Raw samples:

- Clean legacy: `4222, 4324, 4384, 4119, 4140 ns/op`.
- Current legacy control: `4153, 4122, 4219, 4258, 4218 ns/op`.
- Synchronous space: `4275, 4260, 4315, 4353, 4284 ns/op`.
- Periodic space: `4656, 4556, 4574, 4828, 4592 ns/op`.
- Disabled space: `317.9, 326.5, 320.2, 316.8, 313.8 ns/op`.

The disabled result is not a free durability optimization: it is faster
because it deliberately does not write a WAL record. The existing default is
therefore retained.
