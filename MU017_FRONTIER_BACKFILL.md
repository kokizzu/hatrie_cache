# M-U17 Frontier-Safe Backfill Handoff

M-U17 adds a frontier handoff for maintained SQL projections. It combines the
existing source-frontier barrier and immutable SQL snapshot provider with
`IncrementalProjectionRunner`, so a backfill can establish one exact replay
boundary before live changes are consumed.

## API

```go
run, err := runner.BackfillAtFrontier(
    ctx,
    []string{"people"},
    barrier,
    7,
)
```

The runner must be enabled and its resolver must implement
`SQLFrontierSnapshotProvider`. The barrier must track every source partition
needed by the provider. On success, `run.ThroughSequence` and
`runner.Checkpoint()` are `7`; a subsequent `Apply` can begin with sequence
`8`.

## Ordering Contract

1. The runner waits until all barrier partitions have observed the requested
   frontier.
2. The resolver opens one immutable snapshot explicitly bound to that
   frontier.
3. Maintained views refresh from that snapshot.
4. The optional checkpoint store durably saves the frontier.
5. Only then does the runner publish the checkpoint and return.

The runner lock spans the wait and publication, preventing a concurrent live
`Apply` from overtaking the handoff. The snapshot release callback is invoked
once after refresh, checkpoint save, or an execution error. If snapshot opening
or checkpoint save fails, the checkpoint does not advance. A refresh may have
already published before a checkpoint-save failure; retrying the same frontier
is safe and is required before consuming later live sequences.

The method does not start or own a change-log consumer. After success, the
caller must attach its live stream at `frontier+1`; the runner's existing
contiguous-sequence and replay checks enforce that boundary. Disabled runners
return without waiting or opening a snapshot.

## Scope and Safety

This is an importable embedded coordination primitive. It does not persist
source history, implement connector-specific snapshotting, or claim that a
caller-provided snapshot provider is correct beyond its explicit contract. The
ordinary `Rebuild` API remains available for trusted recovery snapshots, while
M-U17 is the path for a source/frontier-aware handoff.

## Verification

The focused tests cover exact snapshot frontier selection, one-time release,
checkpoint advancement, `frontier+1` live continuation, snapshot failures,
checkpoint-save failures, and no checkpoint advancement on failure. Commands:

```text
make test-mu017-frontier-backfill
make test-mu017-frontier-backfill-package
make race-mu017-frontier-backfill
make vet-mu017-frontier-backfill
make benchmark-mu017-baseline
make benchmark-mu017-frontier-backfill
```

## Benchmark

Five pinned runs used `GOMAXPROCS=1` and `-benchtime=500ms` on Linux/amd64,
AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Pre-change trusted `Rebuild` | 5,801 | 4,920 | 29 |
| M-U17 `BackfillAtFrontier` | 6,084 | 4,920 | 29 |

The handoff path has the same measured memory and allocation footprint and is
approximately 1.05x slower in this synthetic workload. That CPU cost is the
frontier wait/provider boundary, reported honestly; the feature's value is
correctness at the snapshot-to-live transition, not raw speed over an already
trusted rebuild.
