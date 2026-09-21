# M208: Differential Multiplicity Folding

M208 adds differential folding for query-subscription batches. Repeated
changes for the same row are combined by signed multiplicity before the batch
is applied or transferred to a consumer.

## API

- `FoldQuerySubscriptionDeltas` folds a slice of deltas and returns detached
  rows.
- `FoldQuerySubscriptionDeltaBatch` folds a batch while preserving its
  columns and metadata.
- Equal canonical rows are summed with checked `int64` arithmetic.
- Zero-multiplicity rows are removed.
- The first surviving row order is retained.
- Input rows and metadata are not mutated.

The M206 upsert-envelope adapter and the Debezium-compatible changefeed
adapter use the private no-copy form of the same implementation. The
adapters still enforce their existing envelope semantics: folding can cancel
or reduce repeated changes, but an unsupported final multiplicity is still
rejected.

The common two-delta path avoids the hash map and canonical-key formatting for
ordinary unequal rows. Values containing `NaN` use the canonical-key fallback
so equal-key behavior remains correct. Larger batches use a hash map rather
than sorting and allocating a sortable copy.

## Correctness coverage

Tests cover stable ordering, cancellation, positive and negative multiplicity,
metadata preservation, input isolation, integer overflow, `NaN` keys, and
integration with both changefeed adapters. The focused race and vet targets
also pass.

## Benchmark

Command:

```text
make benchmark-m208-differential-folding
```

Environment: Linux amd64, AMD Ryzen 9 5950X, five samples per benchmark,
`-benchtime=2s`.

| Implementation | Raw ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Sort baseline | 6002789, 6064551, 5684211, 5196220, 5751509 | 5751509 | ~1435600 | 49410 |
| Hash differential fold | 2450988, 2421232, 2478994, 2525699, 2310452 | 2450988 | 942840 | 24956 |

The hash fold is approximately **2.35x faster**, uses **1.52x fewer bytes**,
and uses **1.98x fewer allocations** for this 4096-delta workload.

The integrated M206 adapter check was also rerun after the optimization:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Debezium baseline | 4861 | 2822 | 34 |
| M206 upsert envelope | 4623 | 2389 | 32 |

That comparison is approximately **1.05x faster**, **1.18x fewer bytes**, and
**1.06x fewer allocations** for the adapter workload.
