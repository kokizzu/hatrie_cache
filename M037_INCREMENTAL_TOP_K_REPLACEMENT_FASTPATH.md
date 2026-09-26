# M037 Incremental Top-K Replacement Fast Path

## Adopted idea

This applies three database-engine patterns to a common update shape:

- ClickHouse-style bounded Top-K work keeps the operation focused on the
  selected prefix rather than rebuilding unrelated rows.
- Materialize-style differential maintenance treats a retraction followed by
  an insertion for one key as one state transition.
- Tarantool-style tuple updates keep stable-key replacement local to the
  existing indexed entry when the identity does not change.

`IncrementalTopK.apply` now recognizes an existing key with exactly two
updates, negative then positive, and validates the replacement before taking
the existing before/after selection snapshots. It skips the temporary pending
map and prepared-update slice. All other batches retain the generic path.

## Measurement

Workload: `BenchmarkMZ037TopKIncremental`, five benchmark samples, AMD Ryzen
9 5950X, `-benchmem`.

| Version | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before fast path | 1,446 | 572 | 5 |
| After fast path | 1,277 | 444 | 4 |
| Change | 1.13x faster | 22.4% lower | 20.0% lower |

The benchmark maintains 10,000 weighted rows with `K=20` and repeatedly
replaces one key's score. The published Top-K result and change rows remain
unchanged in meaning.

## Verification

- Red regression test first failed because `applyReplacement` did not exist.
- Focused replacement test passed after implementation.
- All Top-K tests passed.
- Full `hat/hatSql` package tests passed.
- Race-selected Top-K tests passed.
- Temporary benchmark/test directories use traps and were removed after each
  run; the repository temporary-directory audit found no stale candidates.

## Tradeoffs

The branch is deliberately limited to the exact same-key replacement shape.
It adds a small amount of specialized code but does not alter treap ordering,
row cloning, multiplicity validation, overflow checks, scratch retention, or
the generic multi-key path.
