# MZ-025 TAIL Progress And Heartbeats

The query subscription API already supports opt-in frontier progress through
`QuerySubscriptionDefinition.EmitProgress`. `QuerySubscriptions.Heartbeat`
adds the missing idle-source operation: it emits a progress-only record without
re-evaluating any query or requiring a fake data dependency change.

## API

```go
func (registry *QuerySubscriptions) Heartbeat(frontier uint64) error
```

Only subscriptions created with `EmitProgress: true` receive a heartbeat.
Normal subscriptions are untouched. The heartbeat:

- sets `Progress` on the delivered snapshot or differential batch;
- advances the observed frontier monotonically, never backward;
- leaves `Revision` and query results unchanged;
- sends no differential row deltas;
- completes an `UpTo` subscription when the heartbeat reaches its bound; and
- returns an error for a zero frontier or nil registry.

Heartbeats use the existing bounded update queues. A slow consumer can observe
the newest progress record after stale queued records are coalesced; a
heartbeat is therefore a liveness/progress signal, not a durable delivery
acknowledgement.

## Example

```go
resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
	return []hatSql.Row{{"name": key}}, nil
})
registry := hatSql.NewQuerySubscriptions(1)
subscription, _ := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
	Query:        "FROM CACHE('people') SELECT name",
	Dependencies: []string{"people"},
	EmitProgress: true,
}, resolver, hatSql.QueryOptions{})
defer subscription.Close()

registry.Heartbeat(42)
update := <-subscription.Updates()
fmt.Println(update.Progress, update.Frontier, update.Revision)
```

Output:

```text
true 42 1
```

For differential subscriptions, the same heartbeat is delivered as
`QuerySubscriptionDeltaBatch{Progress: true, Deltas: nil}`. A source adapter
can call `Heartbeat` after advancing its source frontier even when no rows were
inserted, updated, or deleted.

## Cost

The benchmark compares the new direct operation with the previous way to
produce idle progress, calling `NotifyChangedAt` with no changed dependencies.
It uses 64 progress-enabled subscriptions, five `-benchmem` samples, and an
AMD Ryzen 9 5950X on Linux `amd64`.

| Operation | Median CPU | Allocated memory | Allocations |
| --- | ---: | ---: | ---: |
| Idle `NotifyChangedAt` progress | 8,230 ns/op | 10,752 B/op | 2 allocs/op |
| `Heartbeat` progress | 4,235 ns/op | 512 B/op | 1 alloc/op |

That is about 1.94x faster, 21x lower allocated memory, and 2x fewer
allocations for idle progress publication. Reproduce with:

```text
make benchmark-mz025
```

