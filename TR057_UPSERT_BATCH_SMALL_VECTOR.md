# Small-Vector Upsert Batch

`UpsertBatch` now keeps batches with at most 16 distinct keys in their
existing insertion-ordered record slice and performs a bounded linear key
lookup. When a 17th distinct key is added, the batch promotes its existing
records to the original map-backed index. A constructor capacity above 16
continues to select the pre-sized map path directly.

The change is automatic and does not alter the public API, first-seen order,
last-write-wins behavior, tombstones, `Reset`, or zero-value use. `Reset`
retains the record backing array and, after promotion, the map for reuse.
Large batches therefore keep the existing map representation; only small
batches avoid map buckets and their associated allocation.

## Measurement

The benchmark was run on Linux/amd64 with an AMD Ryzen 9 5950X. Each result
below is the median of three samples from
`make benchmark-upsert-batch-small-vector-c210`. The steady-state benchmark
resets and reuses a batch; the fresh benchmark constructs a batch for every
sampled operation. The baseline source was captured from `HEAD` before this
feature, and the final source was restored and measured with the same
benchmark.

### Steady State

| Distinct keys | Before | After | Relative result | Before memory | After memory | Before allocations | After allocations |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 27.86 ns/op | 8.648 ns/op | 3.22x faster | 0 B/op | 0 B/op | 0 | 0 |
| 4 | 95.75 ns/op | 43.54 ns/op | 2.20x faster | 0 B/op | 0 B/op | 0 | 0 |
| 8 | 204.7 ns/op | 136.8 ns/op | 1.50x faster | 0 B/op | 0 B/op | 0 | 0 |
| 16 | 424.7 ns/op | 347.3 ns/op | 1.22x faster | 0 B/op | 0 B/op | 0 | 0 |
| 32 | 822.8 ns/op | 824.8 ns/op | 1.00x, 0.2% slower | 0 B/op | 0 B/op | 0 | 0 |
| 64 | 1,675 ns/op | 1,676 ns/op | 1.00x | 0 B/op | 0 B/op | 0 | 0 |
| 1,000 | 27,831 ns/op | 28,077 ns/op | 0.99x, 0.9% slower | 0 B/op | 0 B/op | 0 | 0 |

Raw baseline samples:

```text
batch_1: 31.35 27.86 26.59 ns/op; 0 B/op; 0 allocs/op
batch_4: 95.75 102.4 86.64 ns/op; 0 B/op; 0 allocs/op
batch_8: 180.8 206.1 204.7 ns/op; 0 B/op; 0 allocs/op
batch_16: 426.6 424.7 399.6 ns/op; 0 B/op; 0 allocs/op
batch_32: 822.8 771.8 861.5 ns/op; 0 B/op; 0 allocs/op
batch_64: 1675 1778 1664 ns/op; 0 B/op; 0 allocs/op
batch_1000: 27349 27831 30032 ns/op; 0 B/op; 0 allocs/op
```

Raw final samples:

```text
batch_1: 8.648 9.264 8.044 ns/op; 0 B/op; 0 allocs/op
batch_4: 41.23 43.54 47.52 ns/op; 0 B/op; 0 allocs/op
batch_8: 137.4 136.8 132.9 ns/op; 0 B/op; 0 allocs/op
batch_16: 347.3 377.1 331.4 ns/op; 0 B/op; 0 allocs/op
batch_32: 824.8 848.6 822.8 ns/op; 0 B/op; 0 allocs/op
batch_64: 1717 1676 1620 ns/op; 0 B/op; 0 allocs/op
batch_1000: 27262 28077 28833 ns/op; 0 B/op; 0 allocs/op
```

### Fresh Construction

| Distinct keys | Before | After | Relative result | Before memory | After memory | Before allocations | After allocations |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 130.5 ns/op | 27.53 ns/op | 4.74x faster | 288 B/op | 32 B/op | 3 | 1 |
| 4 | 228.4 ns/op | 79.61 ns/op | 2.87x faster | 384 B/op | 128 B/op | 3 | 1 |
| 16 | 761.5 ns/op | 464.2 ns/op | 1.64x faster | 1,496 B/op | 512 B/op | 5 | 1 |
| 32 | 1,383 ns/op | 1,402 ns/op | 0.99x, 1.4% slower | 3,032 B/op | 3,032 B/op | 5 | 5 |

The 1/4/16-entry fresh cases reduce allocated bytes by 88.9%, 66.7%, and
65.8%, respectively, while removing two or four allocations. The 32-entry
case remains map-backed and retains the same measured allocation shape. The
steady-state map controls have no allocation change; their small CPU
differences are within run-to-run benchmark variance.

Commands:

```text
make test-upsert-batch-small-vector-c210
make benchmark-upsert-batch-baseline-c210
make benchmark-upsert-batch-small-vector-c210
make benchmark-upsert-batch-control-c210
make verify-upsert-batch-small-vector-c210
```
