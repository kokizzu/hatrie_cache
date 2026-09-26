# M064 Recursive Reachability Single-Edge Fast Path

## Adopted idea

This follows Materialize-style incremental recursive maintenance: when one new
edge arrives, propagate only the affected source/destination frontier instead
of constructing batch bookkeeping for a one-item batch. It also matches
Tarantool's preference for keeping a stable append operation local to its
indexed state.

`IncrementalRecursiveReachability.Append` now uses a dedicated single-edge
path. It validates and rejects duplicate edges exactly as before, updates the
edge/ancestor/reachability maps atomically, and preserves the existing sorted
transitive output. The common case with no existing ancestor or descendant
frontier emits the direct pair without endpoint slices.

Multi-edge batches continue through the existing pending-map path.

## Measurement

Workload: `BenchmarkRecursiveReachabilityMaintenance/incremental_append`,
five benchmark samples, AMD Ryzen 9 5950X, `-benchmem`.

| Version | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before fast path | 2,087 | 1,118 | 14 |
| After fast path | 1,988 | 1,086 | 12 |
| Change | 1.05x faster | 2.9% lower | 14.3% lower |

The benchmark seeds 1,024 edges and repeatedly appends one new edge from an
existing root to a new leaf.

## Verification

- Red regression test first failed because `appendSingleLocked` did not exist.
- Focused single-edge test passed after implementation.
- Recursive reachability tests passed, including transitive closure, cycles,
  duplicate suppression, and atomic validation.
- Full `hat/hatSql` package tests passed.
- Race-selected recursive reachability tests passed.
- Temporary benchmark/test directories use traps and were removed after each
  run; the repository temporary-directory audit found no stale candidates.

## Tradeoffs

The new branch adds a small specialized helper and deliberately applies only
to one-edge batches. It does not change the multi-edge algorithm or add
retained memory. The measured gain is modest, but allocation count falls and
the path preserves the existing recursive state model.
