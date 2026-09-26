# MZ035 Incremental Multiset Same-Key Fast Path

Materialize-style differential updates often arrive as a batch for one logical
key. The generic `IncrementalMultiset` path used a pending map, key sorting, and
per-update row cloning even when every update had the same key. The new path
keeps the same validation and signed-multiplicity rules, folds the batch through
one scalar state machine, and clones a retained row only at publication.

The optimization is automatic for batches with at least two updates sharing
one key. Multi-key batches and single updates keep their existing paths. It is
not a new default or a changed consistency mode; it only removes work when the
input shape proves the optimization is safe.

## Benchmark

Host: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. The fixture contains
64 signed updates for one key (32 insert/retract pairs), so the final
multiplicity is zero. `GenericMap` is the previous generic implementation kept
as a benchmark control; `SameKeyFastPath` is the optimized implementation.

Raw five-sample output after the change:

```text
BenchmarkMZ035SameKeyBatch/GenericMap-32       25736 ns/op  20440 B/op 260 allocs/op
BenchmarkMZ035SameKeyBatch/GenericMap-32       26032 ns/op  20440 B/op 260 allocs/op
BenchmarkMZ035SameKeyBatch/GenericMap-32       26314 ns/op  20440 B/op 260 allocs/op
BenchmarkMZ035SameKeyBatch/GenericMap-32       26547 ns/op  20440 B/op 260 allocs/op
BenchmarkMZ035SameKeyBatch/GenericMap-32       26107 ns/op  20440 B/op 260 allocs/op
BenchmarkMZ035SameKeyBatch/SameKeyFastPath-32   2656 ns/op      0 B/op   0 allocs/op
BenchmarkMZ035SameKeyBatch/SameKeyFastPath-32   2502 ns/op      0 B/op   0 allocs/op
BenchmarkMZ035SameKeyBatch/SameKeyFastPath-32   2544 ns/op      0 B/op   0 allocs/op
BenchmarkMZ035SameKeyBatch/SameKeyFastPath-32   2642 ns/op      0 B/op   0 allocs/op
BenchmarkMZ035SameKeyBatch/SameKeyFastPath-32   2651 ns/op      0 B/op   0 allocs/op
```

Median comparison: about **10.0x lower latency**, **100% fewer measured bytes**,
and **100% fewer measured allocations** for this same-key batch. The existing
single-update benchmark remained within noise (`about 94 ns/op` before versus
`about 90 ns/op` after, with `48 B/op` and `1 alloc/op` in both cases).

## Correctness Checks

- Same-key positive and negative deltas preserve exact multiplicity.
- Invalid batches remain atomic and do not publish partial state.
- Row conflicts and overflow checks remain enforced.
- A published row is cloned once, so later caller mutation cannot alter state.
- Multi-key batches continue through the generic implementation.

Verification run through the repository Makefile-backed script:

```text
focused MZ035 tests: PASS
race MZ035 tests: PASS
full hat/hatSql package: PASS
```
