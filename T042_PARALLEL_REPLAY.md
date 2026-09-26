# T042 Key-Partitioned Parallel Recovery Replay

T042 adds an opt-in replay primitive for recovery workloads whose journal
records are independent by logical key. It is designed for the caller-owned
apply layer around `hatReplication.HotStandby`; existing recovery behavior is
unchanged.

## API

```go
err := hatReplication.ReplayJournalRecordsParallel(
    ctx,
    records,
    hatReplication.ParallelReplayOptions{
        Workers: 8,
        Key: func(record hatJournal.Record) string {
            return record.Request.Key
        },
        Apply: func(ctx context.Context, record hatJournal.Record) error {
            return applyRecord(ctx, record)
        },
    },
)
```

`Workers: 0` or `Workers: 1` selects the serial path. Parallel mode is
bounded to 64 workers and 1,048,576 records per call. Batches smaller than 32
records also use the serial path to avoid paying scheduler and lane setup
costs.

## Ordering And Failure Semantics

- `Key` maps every record to a stable logical key.
- All records with one key run on one lane, in their original input order.
- Different keys may run concurrently; there is no global ordering guarantee.
- `Apply` must be safe for concurrent different-key calls and must not require
  cross-key transaction ordering.
- Context cancellation stops new callback work and cancels sibling lanes after
  an apply error.
- The first callback error is returned. The caller owns retry and idempotency
  policy for records that completed before another lane failed.
- The input record slice must remain immutable until the call returns.

This helper must not be used for commands such as cross-key transactions,
renames, or secondary-index updates whose correctness depends on a global
record order. Those workloads should keep the serial default or provide a
caller-owned transaction-aware applier.

## Resource Bounds

The implementation hashes keys to fixed lanes and stores record indexes rather
than copying full journal records. The final benchmark allocates about 96 KB
for a 10,000-record batch, while the serial default allocates nothing in the
replay helper. There is no hidden background goroutine after the call returns.

## Benchmark

Command:

```text
make benchmark-t042-parallel-replay
```

Host: Linux/amd64, AMD Ryzen 9 5950X. Each case uses 10,000 `SET` records
spread across 64 independent keys, eight replay workers, and five samples.
The callback performs the same CPU work in both cases.

| Operation | Median ns/op | B/op | Allocs/op | Raw ns/op samples |
| --- | ---: | ---: | ---: | --- |
| Serial baseline | 2,262,314 | 0 | 0 | 2,262,314; 2,374,584; 2,070,900; 2,252,808; 2,320,478 |
| Parallel key lanes | 669,342 | 96,211 | 25 | 669,342; 656,636; 672,645; 670,849; 658,028 |

The opt-in parallel path is about 3.38x faster for this independent-key
workload. Its cost is approximately 96 KB and 25 allocations per 10,000-record
batch; the default serial path retains zero allocations.

The first implementation copied complete records into lane slices. It was
rejected after measuring about 2.12 ms/op, 7.16 MB/op, and 112 allocations,
with no speedup over serial replay. The final implementation partitions compact
record indexes instead, and the rejected result is retained here to prevent
regression toward that tradeoff.

## Verification

```text
make test-t042-parallel-replay
make race-t042-parallel-replay
make vet-t042-parallel-replay
make test-t042-package
```
