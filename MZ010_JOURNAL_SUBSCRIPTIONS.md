# MZ-010 Journal Subscriptions

Status: partially adopted.

This is the first Materialize-inspired subscription primitive in `hatrie_cache`.
It exposes a bounded, opt-in stream of durable `CommandJournalRecord` values.
It is intentionally lower-level than Materialize `TAIL`: it does not evaluate a
SQL query, emit differential rows, or maintain a SQL result projection.

## API

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

subscription, err := journal.Subscribe(ctx, hatCache.CommandJournalSubscribeOptions{
	AfterSequence: checkpoint,
	ReplayLimit:   1000,
	Buffer:        256,
	PollInterval:  10 * time.Millisecond,
})
if err != nil {
	return err
}
defer subscription.Close()

for record := range subscription.Records() {
	if err := apply(record); err != nil {
		return err
	}
}
return subscription.Err()
```

`AfterSequence` is exclusive. A value of `0` starts after the beginning of the
journal. `ReplayLimit` bounds the initial durable replay; zero selects the
existing command-journal tail default. `Buffer` is the bounded output channel
capacity; zero selects `256` and the maximum is `65536`. `PollInterval` is a
fallback check interval; zero selects `10ms`. Normal journal writes notify
subscribers directly, so this interval is normally not on the live path.

## Delivery Semantics

1. The subscription is registered before its initial tail is read, preventing a
   concurrent append from falling between registration and replay.
2. Existing records after `AfterSequence` are delivered in sequence order.
   A replay that exceeds `ReplayLimit` is rejected with
   `ErrCommandJournalSubscriptionReplayLimit`.
3. Ordinary commands and group-commit commands publish their durable record
   directly to each active subscription. Batch and other internal journal
   paths publish a wake-up; the subscriber then reads the durable tail.
4. Duplicate or already replayed notifications are ignored. A sequence gap is
   repaired from the durable tail while the journal is open.
5. The output channel is bounded. If a live subscriber cannot accept its next
   record, the subscription stops and `Err()` returns
   `ErrCommandJournalSubscriptionOverflow`. This prevents an inactive consumer
   from retaining an unbounded queue.

`Close` stops a subscription and waits for `Records` to close. Context
cancellation is reported through `Err`. Closing the journal terminates idle
subscriptions promptly with `ErrCommandJournalClosed`; records already queued
for delivery may be drained first. A subscription cannot repair a sequence gap
after journal close. If compaction removed the requested history,
`ErrCommandJournalCompacted` is returned and the consumer must restart from a
new snapshot/checkpoint.

## Operational Guidance

Use a persisted sequence checkpoint and make the consumer idempotent. A
subscription is a delivery mechanism, not a distributed acknowledgment
protocol. Do not expose raw records to untrusted clients: journal records can
contain command keys, values, and internal replication data. Put authentication
and authorization at the owning service boundary.

Subscriptions are opt-in. With no active subscribers, the write path performs
only an atomic subscriber-count check and does no channel allocation or journal
scan. Each active subscriber adds one goroutine, one bounded output channel,
one wake slot, and O(active subscribers) notification work per directly
published record. Larger output buffers tolerate bursts but retain more data;
smaller buffers fail faster and bound memory more tightly.

## Scope Still Open

The following are not part of this API:

- SQL `TAIL` or `SUBSCRIBE` statements.
- Differential `INSERT`/`DELETE`/`UPDATE` rows for a query result.
- Exactly-once sink acknowledgments coupled to a query frontier.
- Cross-process subscription transport over HTTP/2 or gRPC.

Those features require a query projection, schema for change envelopes, and a
checkpoint/acknowledgment contract. They should be designed separately from
this command-journal transport primitive.

## Measurements

The benchmark and raw five-sample output are in
[BENCHMARK.md](BENCHMARK.md#mz-010-command-journal-subscriptions). The direct
event path was also compared with the initial polling-only prototype: it was
approximately `3.0x` faster, used approximately `532x` fewer allocated bytes,
and used approximately `492x` fewer allocations in the live-record workload.
That prototype comparison is a development measurement, not a release-to-release
compatibility guarantee.
