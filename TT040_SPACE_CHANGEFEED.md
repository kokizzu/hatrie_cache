# TT-040 Space Changefeed

`CommandJournal.SubscribeSpace` provides an opt-in changefeed for one logical
space. The current journal model uses the exact command `Key` as the space
identifier, so callers can maintain one projection per key without receiving
records for unrelated keys.

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
