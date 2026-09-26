# M038 Small-Batch Differential Consolidation

Materialize-style differential arrangements consolidate updates for the same
logical keys before publishing a new state. `IncrementalMultiset` already had
an exact map-and-sort implementation and a same-key fast path. This change adds
a bounded slice accumulator for non-same-key batches containing at most eight
updates.

## Admission and behavior

- `Apply` still checks the existing single-update and same-key paths first.
- A non-same-key batch with two through eight updates uses fixed local arrays,
  linear key lookup, and insertion ordering. It publishes only after every
  update validates successfully.
- Larger batches retain the generic map-and-sort implementation.
- Output ordering, row isolation, timestamps, multiplicity overflow handling,
  negative-multiplicity rejection, and atomic failure behavior are unchanged.

The bounded slice path uses O(k^2) comparisons for `k <= 8`; this is the
deliberate tradeoff that avoids a temporary map, key slice, and sort closure on
small batches. No additional retained state is introduced.

## Benchmark

Host: Linux/amd64, AMD Ryzen 9 5950X. Each value is the median of five samples
from `go test -benchmem -count=5`. The benchmark applies positive updates to
the same fixed keys and compares the unchanged generic map path with the new
slice path.

| Shape | Path | ns/op | B/op | allocs/op | Relative latency |
| --- | --- | ---: | ---: | ---: | ---: |
| 2 keys x 2 updates | GenericMap | 3,670 | 2,480 | 37 | 1.00x |
| 2 keys x 2 updates | SmallSlice | 3,011 | 1,808 | 33 | 1.22x faster |
| 4 keys x 2 updates | GenericMap | 7,333 | 5,024 | 74 | 1.00x |
| 4 keys x 2 updates | SmallSlice | 5,983 | 3,616 | 65 | 1.23x faster |
| 8 keys x 1 update | GenericMap | 10,343 | 9,280 | 98 | 1.00x |
| 8 keys x 1 update | SmallSlice | 8,184 | 6,464 | 81 | 1.26x faster |

The final path reduces measured bytes by 27% to 30% and allocations by 11% to
17% across the tested shapes. The pre-change four-key generic baseline was
7,572 ns/op, 5,024 B/op, and 74 allocs/op; the generic implementation is
unchanged and remained within normal run-to-run variation after the addition.

## Verification

The focused regression test compares the slice and generic states and deltas,
checks deterministic ordering, caller-row isolation, rejected-batch atomicity,
and overflow atomicity. Verification completed with:

```text
make test-m038-small-batch
ok   hatrie_cache/hat/hatSql  0.006s

make race-m038-small-batch
ok   hatrie_cache/hat/hatSql  1.028s

make test-m038-sql-package
ok   hatrie_cache/hat/hatSql  4.298s
```

The focused test was run red before implementation and failed because
`applySmallBatch` did not exist; it passed after the implementation.
