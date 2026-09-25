# TT-006 Hot-Standby WAL Catch-Up

## Status

Partially adopted as an importable, transport-neutral continuous replay runner
in `hat/hatReplication`. It continuously fetches bounded journal batches,
validates that they are contiguous after the installed snapshot sequence, and
passes them to a caller-owned durable applier. A caller-owned fencing and
topology layer still performs the final write publication during promotion.

## Why

Tarantool-style hot standby reduces recovery time by keeping a read-only copy
near the source instead of waiting for a full restore after failure. The
runner starts from `StartSequence`, so it can be placed after an existing
snapshot-plus-WAL bootstrap.

## API

`NewHotStandby` uses these defaults when values are zero:

- batch size: `256` records;
- idle poll interval: `100ms`;
- fencing token: required and non-zero.

`Run` is blocking and has no hidden goroutine. The embedding service owns the
goroutine and cancels its context when the source is fenced or the standby is
being stopped. `HotStandbySource.Fetch` returns a source high-water sequence
and records strictly after the requested sequence. `HotStandbyApplier.Apply`
must make each batch durable or roll it back before returning.

The runner rejects:

- records that do not start at `AppliedSequence + 1`;
- gaps or duplicate/out-of-order record sequences;
- an empty batch whose source high-water mark is ahead of the standby;
- a source sequence regression;
- batches larger than the configured limit;
- promotion while replay is running, stopped with non-zero lag, or using a
  stale generation/fencing token.

`Promote` changes only the local state machine. The caller must fence the old
source with the same token and atomically publish the promoted node as the
writer. No network, election, or storage engine is hidden inside this API.

## Journal Adapter Shape

The existing `hatCache.CommandJournal.Tail(after, limit)` API can be adapted
without copying records:

```go
source := hatReplication.HotStandbyFetchFunc(func(
    ctx context.Context, after uint64, limit int,
) (hatReplication.HotStandbyBatch, error) {
    tail, err := journal.Tail(after, limit)
    if err != nil {
        return hatReplication.HotStandbyBatch{}, err
    }
    return hatReplication.HotStandbyBatch{
        SourceSequence: tail.LastSequence,
        Records:        tail.Entries,
    }, nil
})
```

The applier remains storage-specific: it should apply the validated records
using the standby's normal journal/replay transaction and persistence barrier.
This separation avoids making the replication package depend on the cache
execution package or silently bypassing existing durability rules.

## Benchmark

Command:

```text
make benchmark-tt006-hot-standby
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X, with 64 one-record batches:

| Path | Time | Memory | Allocations |
| --- | ---: | ---: | ---: |
| Hot-standby runner, fetch/validate/apply/status loop | 11.820-12.243 us/op | 14,720 B/op | 69 allocs/op |
| Raw contiguous sequence loop control | 20.25-22.20 ns/op | 0 B/op | 0 allocs/op |

The raw loop is a lower-bound control, not a replacement for replication: it
does no locking, callback dispatch, timestamps, context handling, polling, or
error-state tracking. The hot-standby path is opt-in and adds no overhead to
ordinary commands when unused. The measured cost is the tradeoff for bounded
gap detection, cancellation, observability, and fenced promotion.

## Verification

The focused tests cover contiguous replay and promotion, malformed sequence
rejection without applying data, source watermark retention on failure,
validation defaults, cancellation, stale generations, and fencing. Run:

```text
make test-tt006-hot-standby
make test-tt006-package
make race-tt006-hot-standby
make vet-tt006-hot-standby
make compile-tt006-all
```
