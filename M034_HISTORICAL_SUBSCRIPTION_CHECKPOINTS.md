# M-U34 Historical Subscription Checkpoints

M-U34 adds an opt-in cancellation and resume protocol for historical SQL
subscriptions. It is intended for a consumer that may be interrupted while
replaying a bounded `AS OF`/frontier stream.

## Contract

1. Read the initial snapshot and call `Acknowledge` only after the consumer has
   applied it.
2. Read an update and acknowledge it after applying the update.
3. Call `CloseWithCheckpoint` when stopping. It atomically records the last
   acknowledged revision/frontier and closes the subscription.
4. Persist the returned `QuerySubscriptionCheckpoint` with the application's
   backup or checkpoint store.
5. Restore it with `Resume` or `ResumeDifferential`.

```go
subscription, err := registry.Subscribe(ctx, definition, resolver, options)
if err != nil {
	return err
}

initial, _ := subscription.Snapshot()
if err := subscription.Acknowledge(initial); err != nil {
	return err
}

// After applying an update from subscription.Updates():
// if err := subscription.Acknowledge(update); err != nil { return err }

checkpoint, err := subscription.CloseWithCheckpoint()
if err != nil {
	return err
}
// Store checkpoint in the application's durable checkpoint/backup record.

resumed, err := registry.Resume(checkpoint)
if err != nil {
	return err
}
defer resumed.Close()
```

`Resume` does not evaluate the query at the saved frontier. A repeated
`NotifyChangedAt` at or below the saved frontier is ignored, and the next
frontier refreshes from the saved result. `ResumeDifferential` does not emit an
initial batch because that batch is represented by the consumer's checkpoint.

The checkpoint is detached and versioned (`Version == 1`). It contains the
normalized query definition, the consumer-applied result, the frontier,
revision, and whether the subscription is differential. The library does not
choose a persistence format or retain checkpoints itself; callers can store it
alongside their existing backup/frontier record.

An acknowledgement must identify the latest producer revision and frontier.
Acknowledging a stale queued update, acknowledging a different result, or
checkpointing before the first acknowledgement fails without closing the
subscription. This prevents a stop/restart cycle from skipping a result that
was delivered but not applied.

`Close` and all existing subscription APIs remain unchanged. The checkpoint
state is allocated lazily only after the opt-in acknowledgement path is used.

## Measurement

Five `-benchtime=200ms` samples on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Median time | Heap/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Existing `Snapshot` + `Close` | 10,389 ns | 8,136 B | 47 | 1.00x |
| `Acknowledge` + `CloseWithCheckpoint` | 12,301 ns | 9,256 B | 67 | 1.18x, +13.8% heap |

The second row is an explicit durability cost, not a replacement for the
legacy path. Ordinary subscriptions that never acknowledge a checkpoint keep
the existing behavior and do not allocate checkpoint state.

## Verification

```text
make test-mu034
make race-mu034
make vet-mu034
make benchmark-mu034
```

The tests cover unacknowledged queued updates, repeated frontier delivery,
ordinary and differential resume, detached checkpoint data, and failed
checkpoint attempts that must leave the live subscription active.
