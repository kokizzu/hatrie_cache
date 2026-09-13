# CH-09: Bounded Async Insert Buffer

This is a ClickHouse-inspired ingest feature for workloads that produce many
small writes. It is opt-in: constructing `hatCache.AsyncInsertBuffer` is the
only way to enable it. Existing command, HTTP, gRPC, journal, and server
defaults remain unchanged.

## Behavior

`NewAsyncInsertBuffer` accepts a `CommandJournal`, a `HatTrie`, and
`AsyncInsertBufferOptions`:

| Option | Default | Purpose |
| --- | ---: | --- |
| `BatchSize` | `64` | Number of accepted writes handed to the journal during one flush. |
| `Capacity` | `4096` | Maximum number of admitted writes waiting for the flusher. |
| `FlushInterval` | `5ms` | Maximum timer interval before a partial group is flushed. |

The buffer accepts journalable writes recognized by the existing command
journal. Reads, nested `BATCH` requests, and per-request idempotency keys are
rejected because the buffer cannot preserve their individual semantics.
Requests are copied before `Submit` returns, so callers may reuse or mutate
their input values.

Flush groups are submitted to the existing asynchronous journal group-commit
queue as individual records. This is intentional: the current compact binary
journal format stores scalar command records, so CH-09 does not introduce a new
on-disk format or pretend that a flush group is one atomic transaction.

Each `AsyncInsertSubmission` reports the response for its own command after
that command is durably appended and applied. A successful `Submit` cannot be
canceled after admission. A context passed to `Wait`, `Flush`, or `Close` only
limits how long the caller waits.

## Example

```go
journal, err := hatCache.OpenCommandJournalWithOptions(
    "data/commands.journal",
    hatCache.CommandJournalOptions{GroupCommitMaxBatch: 64},
)
if err != nil {
    return err
}
defer journal.Close()

trie := hatCache.CreateHatTrie()
defer trie.Destroy()

buffer, err := hatCache.NewAsyncInsertBuffer(journal, trie, hatCache.AsyncInsertBufferOptions{})
if err != nil {
    return err
}
defer buffer.Close(context.Background())

receipt, err := buffer.Submit(context.Background(), hatCache.CacheCommandRequest{
    Command: "SETSTR",
    Key:     "user:42",
    Value:   "ready",
})
if err != nil {
    return err
}

if err := buffer.Flush(context.Background()); err != nil {
    return err
}
response, err := receipt.Wait(context.Background())
if err != nil {
    return err
}
if !response.OK {
    return errors.New(response.Message)
}
```

Use `ErrAsyncInsertBufferFull` as bounded backpressure. A producer can wait,
call `Flush`, or apply its own retry policy rather than allowing unbounded
heap growth.

## Recovery

CH-09 uses the existing journal format and replay path. A normal shutdown is:

1. Stop new producers.
2. Call `Close` and wait for it to return.
3. Close the journal after the buffer is closed.

On restart, reopen the journal and call its existing `Replay` method. Because
each write remains an ordinary journal record, backup, restore, replay,
sequence tracking, and older journal tooling continue to use their existing
paths.

## Measurements

Commands used:

```text
make benchmark-ch009-before
make benchmark-ch009-after
```

Both targets use Go `-benchmem`, `-benchtime=1000x`, and `-count=5` on an AMD
Ryzen 9 5950X. Values below are the five raw runs from the focused benchmark;
the median is used for the ratio.

### Caller admission

| Path | Raw ns/op | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `SubmitAsyncCommand` admission | 10877, 11172, 12244, 19176, 14079 | 12244 | 1139 | 4 |
| CH-09 `buffer.Submit` | 249.7, 203.0, 135.1, 107.9, 130.2 | 135.1 | 299 | 1 |

The buffer admission path is about **90.6x faster**, uses about **3.8x less
heap allocation**, and uses **4x fewer allocations** than direct async
submission. This excludes durability wait time by design.

### Durability path

The existing synchronous baseline reports one write per operation. CH-09's
`submit_flush_64` reports 64 writes per operation; the per-write values are
derived by dividing the raw batch result by 64.

| Path | Raw ns/op | Derived ns/write | Derived B/write | Derived allocs/write |
| --- | --- | ---: | ---: | ---: |
| Existing synchronous journal write | 34575, 43801, 37515, 32837, 31853 | 34575 | 884 | 3.00 |
| CH-09 `submit_flush_64` | 825621, 803464, 807082, 797937, 849875 | 12611 | 1290 | 4.19 |

The measured flush path is about **2.7x faster per write** than the synchronous
baseline because the journal can group the 64 submissions into fewer sync
windows. It allocates about **1.46x more bytes** and **1.40x more allocation
objects per write** than that synchronous baseline. The feature is therefore
useful for latency/throughput-sensitive ingest, but it is not a general
replacement when minimum allocation volume is the primary objective.

There is no storage or wire bandwidth win in this implementation: the same
scalar binary journal record is written for every command. The gain comes from
bounded admission and existing journal group commit. The benchmark does not
claim an RSS measurement; `B/op` is cumulative Go heap allocation, while the
configured capacity bounds outstanding request memory.

## Verification

Focused correctness and race checks:

```text
make format-ch009
make test-ch009
make test-ch009-race
```

The tests cover full and partial flushes, caller request ownership, replay
after reopen, response cloning, bounded admission, cancellation, idempotency
rejection, close flushing, and unsupported journal modes.
