# Durable Asynchronous-Insert Deduplication

CH-U01 extends the opt-in `hatCache.AsyncInsertBuffer` with durable
idempotency keys. A keyed write is fingerprinted by the existing command
journal, recorded with the journal entry, and restored when the journal is
replayed after a restart. A retry with the same key and identical request is
acknowledged without appending another journal record or applying the write a
second time.

## Enable It

Configure a positive journal idempotency capacity and use a bounded async
insert buffer:

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	GroupCommitMaxBatch: 64,
	IdempotencyCapacity: 1024,
})
if err != nil {
	return err
}

buffer, err := hatCache.NewAsyncInsertBuffer(journal, trie, hatCache.AsyncInsertBufferOptions{})
if err != nil {
	return err
}

submission, err := buffer.Submit(ctx, hatCache.CacheCommandRequest{
	Command:        "SET",
	Key:            "orders:42",
	Value:          "accepted",
	IdempotencyKey: "order-import-42",
})
```

`IdempotencyCapacity` defaults to `0`, so the ledger is disabled unless the
caller explicitly enables it. A keyed async insert against a journal without
the ledger returns `ErrAsyncInsertBufferIdempotencyDisabled`. Keys are trimmed
and bounded by `MaxCommandJournalIdempotencyKeyBytes` (256 bytes).

## Semantics

- Unkeyed async inserts retain the previous behavior and cost.
- A repeated key with the same command payload returns success and does not
  grow the journal.
- Reusing a key with a different command payload returns a failed command
  response and leaves the original value unchanged.
- The ledger is bounded by `IdempotencyCapacity`; old entries are evicted in
  insertion order. Set the capacity to cover the maximum retry window and
  journal retention window needed by the application.
- Journal replay restores the durable fingerprints before later retries are
  admitted. The feature is limited to writes already accepted by the
  journal-backed async insert buffer; it does not add a new server queue or
  automatic routing policy.
- Idempotency keys are stored in the journal request metadata. They should be
  stable request identifiers, not credentials or other secrets.

## Measurement

Five `-benchtime=200ms` samples were collected on Linux `amd64` with an AMD
Ryzen 9 5950X. Each operation submits and flushes 64 `SET` commands with
journal sync overridden to a no-op so the benchmark measures buffer and group
commit handling rather than device latency.

| Path | Median time per 64 commands | Heap/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing unkeyed baseline | 108.382 us | 82,577 B | 268 | 1.00x |
| Unkeyed after CH-U01 | 104.179 us | 82,577 B | 268 | 1.04x faster, same heap/allocations |
| Keyed duplicate retry | 122.221 us | 105,412 B | 404 | 1.17x slower, 1.28x heap, 1.51x allocations vs unkeyed |

The default path has no additional allocations or retained benchmark heap.
The keyed retry cost is intentional and buys bounded durable deduplication;
the restart regression test also verifies that a duplicate does not append a
second journal record.

## Raw Output

Before output from `make benchmark-chu01-before-c242`:

```text
BenchmarkCHU01AsyncInsertUnkeyed-32    	    1845	    109684 ns/op	   0.58 MB/s	   82589 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32    	    1956	    109305 ns/op	   0.59 MB/s	   82573 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32    	    2319	    105553 ns/op	   0.61 MB/s	   82575 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32    	    2466	    106586 ns/op	   0.60 MB/s	   82583 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32    	    2161	    108382 ns/op	   0.59 MB/s	   82577 B/op	     268 allocs/op
```

After output from `make benchmark-chu01-after-c242`:

```text
BenchmarkCHU01AsyncInsertUnkeyed-32           	    2077	    104179 ns/op	   0.61 MB/s	   82577 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32           	    2036	    104826 ns/op	   0.61 MB/s	   82576 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32           	    2368	    105179 ns/op	   0.61 MB/s	   82580 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32           	    2103	    101010 ns/op	   0.63 MB/s	   82570 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertUnkeyed-32           	    2140	    102365 ns/op	   0.63 MB/s	   82579 B/op	     268 allocs/op
BenchmarkCHU01AsyncInsertKeyedDuplicate-32    	    1708	    122221 ns/op	   0.52 MB/s	  105411 B/op	     404 allocs/op
BenchmarkCHU01AsyncInsertKeyedDuplicate-32    	    2060	    122984 ns/op	   0.52 MB/s	  105399 B/op	     404 allocs/op
BenchmarkCHU01AsyncInsertKeyedDuplicate-32    	    1850	    122574 ns/op	   0.52 MB/s	  105412 B/op	     404 allocs/op
BenchmarkCHU01AsyncInsertKeyedDuplicate-32    	    1947	    121187 ns/op	   0.53 MB/s	  105474 B/op	     404 allocs/op
BenchmarkCHU01AsyncInsertKeyedDuplicate-32    	    1988	    118770 ns/op	   0.54 MB/s	  105414 B/op	     404 allocs/op
```
