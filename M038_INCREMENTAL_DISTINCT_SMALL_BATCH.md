# M038 Incremental Distinct Small-Batch Consolidation

Materialize-style differential arrangements benefit from consolidating a small
batch without constructing a general-purpose hash arrangement. The existing
`IncrementalDistinct` operator used a map for every multi-update batch, even
when the batch contained only a few keys. It now admits a bounded slice path
for batches of at most eight updates.

## Behavior and guardrails

- Single updates retain the existing direct path.
- Two through eight updates use fixed local arrays and linear key lookup.
- Change rows remain in the original input order, including repeated `+1` and
  `-1` transitions for one key.
- Retained rows from the operator are reused without cloning; caller-owned rows
  are cloned at publication and in returned transition rows.
- Batches larger than eight updates retain the generic map path.
- Validation remains atomic: key errors, row conflicts, negative multiplicity,
  and overflow publish no state.

The bounded path has O(k^2) key lookup for `k <= 8`, with no new retained
memory. The generic path remains available for larger batches.

## Benchmark

Host: Linux/amd64, AMD Ryzen 9 5950X. Each result is the median of five
`go test -benchmem -count=5` samples. The workload contains four keys, each
with an insert followed by a delete, so it exercises eight transition rows and
returns to an empty state on every iteration.

| Path | ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| GenericMap | 2,927 | 4,352 | 25 | 1.00x |
| SmallSlice | 1,973 | 3,008 | 17 | 1.48x faster |

The pre-change public `Apply` baseline was 2,907 ns/op, 4,352 B/op, and 25
allocs/op. The generic implementation remained unchanged after refactoring;
its post-change median was 2,927 ns/op, within normal run-to-run variation.
The bounded path reduces measured bytes by 31% and allocations by 32%.

## Verification

The focused tests compare the generic and slice output/state, preserve input
ordering, verify caller-row isolation, and verify rejected-batch atomicity.
The full package run also covers the existing randomized `IncrementalDistinct`
reference test.

```text
make test-m038-distinct-small-batch
ok   hatrie_cache/hat/hatSql  0.006s

make race-m038-distinct-small-batch
ok   hatrie_cache/hat/hatSql  1.026s

make test-m038-distinct-sql-package
ok   hatrie_cache/hat/hatSql  4.198s
```

The focused test was intentionally run red before implementation and failed
because the new generic and slice helpers did not exist.
