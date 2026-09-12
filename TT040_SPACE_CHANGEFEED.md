# TT-040 Space Changefeed

`CommandJournal.SubscribeSpace` provides an opt-in changefeed for one logical
space. The current journal model uses the exact command `Key` as the space
identifier, so callers can maintain one projection per key without receiving
records for unrelated keys. `CommandJournal.Subscribe` additionally supports
literal key-prefix watchers and bounded per-key coalescing for projections
that cover a group of keys.

## Usage

```go
subscription, err := journal.SubscribeSpace(ctx, "orders", hatCache.CommandJournalSubscribeOptions{
	AfterSequence: checkpoint,
	ReplayLimit:   10_000,
	Buffer:        256,
})
if err != nil {
	return err
}
defer subscription.Close()

for record := range subscription.Records() {
	applyOrderMutation(record)
	checkpoint = record.Sequence
}
if err := subscription.Err(); err != nil {
	return err
}
```

`AfterSequence` is exclusive and uses the journal's global sequence. A space
subscription skips unrelated records while advancing its internal cursor, so
the replay limit counts matching records only. The stream delivers records in
commit order and closes on explicit `Close`, context cancellation, journal
shutdown, compaction errors, or buffer overflow. Consumers should persist the
latest global sequence after applying a record and restart from that sequence.

An empty space is rejected. Matching is exact and case-sensitive; whitespace is
not normalized. The feature does not add a new persistence format or network
endpoint: it reuses the existing command journal, subscription buffer, and
error semantics. It is disabled unless a caller explicitly creates a space
subscription, so ordinary writes and unfiltered subscriptions keep their
existing behavior.

## Prefix watchers and coalescing

Use `KeyPrefix` for a case-sensitive literal prefix. It applies to both replay
and live records, and `ReplayLimit` counts matching records rather than all
journal records:

```go
subscription, err := journal.Subscribe(ctx, hatCache.CommandJournalSubscribeOptions{
	AfterSequence: checkpoint,
	KeyPrefix:     "orders/",
	ReplayLimit:   10_000,
	Buffer:        256,
	Coalesce:      true,
})
if err != nil {
	return err
}
defer subscription.Close()

for record := range subscription.Records() {
	applyOrderMutation(record)
}
if err := subscription.Err(); err != nil {
	return err
}
```

`Coalesce` keeps the newest pending record for each exact command key and
delivers retained records in journal sequence order. During replay, every
matching record in the bounded replay window is reduced to one record per key.
During live delivery, pending state is bounded by `Buffer` unique keys; a
larger set of simultaneously pending keys terminates the subscription with
`ErrCommandJournalSubscriptionOverflow`. Repeated updates to an already
pending key replace its record without increasing that bound. `SubscribeSpace`
and `KeyPrefix` may be combined; both filters must match.

Filtered and coalesced subscriptions intentionally allow sequence gaps in the
records channel. Consumers that persist checkpoints should advance their
checkpoint only after applying each delivered record and should rebuild from a
durable checkpoint if they need every intermediate mutation.

## Performance

The benchmark uses 100 committed records, with 50 records for `space:orders`
and 50 for `space:users`, on `linux/amd64` and an AMD Ryzen 9 5950X. Five
samples were collected with `-benchmem`. The unfiltered case drains all 100
records; the space case drains only the 50 matching records.

| Benchmark | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Unfiltered replay of 100 | 83,259 | 69,789 | 521 |
| Space replay of 50 from 100 | 68,699 | 49,305 | 521 |

The filtered subscription is `0.83x` the total CPU and `0.71x` the allocated
bytes because it avoids constructing and delivering unrelated output records;
the allocation count is unchanged. The scanner still reads the journal in
commit order to preserve correctness. The initial page-based implementation
was rejected after it measured roughly `1.3x` CPU and `3.6x` memory for this
workload.

Run the reproducible checks with:

```sh
make test-tt040-space-changefeed
make test-race-tt040-space-changefeed
make benchmark-tt040-space-changefeed
make vet-tt040-space-changefeed
```

## T-G42 Key-Watcher Benchmark

The focused benchmark uses a 256-record journal on `linux/amd64` with an AMD
Ryzen 9 5950X. Final rows below contain the three `-count=3` samples from
`make benchmark-tg42-key-watchers`, with `-benchtime=500ms -benchmem`; the
historical before-feature control retains its original five samples.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Control CPU / new CPU | Control B/op / new B/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Existing exact-key replay, before feature | 127,204; 125,511; 127,951; 130,460; 129,414 | 127,951 | 90,154 | 1,302 | - | - |
| Existing exact-key replay, final control | 131,652; 128,001; 128,106 | 128,106 | 90,188 | 1,302 | - | - |
| Prefix watcher, same 64 matches | 129,165; 126,605; 128,252 | 128,252 | 90,187 | 1,302 | 1.00x | 1.00x |
| Prefix replay, 128 records, uncoalesced | 139,059; 147,350; 148,298 | 147,350 | 117,579 | 1,302 | - | - |
| Prefix replay, 2 newest records, coalesced | 133,097; 139,010; 136,793 | 136,793 | 100,082 | 1,309 | 1.08x | 1.17x |

The prefix path is allocation-neutral versus the final exact-key control. In
the repeated-key workload, coalescing is `1.08x` faster and uses `1.17x` less
per-operation memory than delivering all 128 records, at the cost of seven
additional allocations per subscription setup. The before/after exact-key
rows are included as a control; their small difference is normal benchmark
noise, not a claimed optimization.

The key-watcher checks are:

```sh
make test-tg42-key-watchers
make race-tg42-key-watchers
make benchmark-tg42-key-watchers
make vet-tg42-key-watchers
```
