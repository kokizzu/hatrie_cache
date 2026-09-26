# M038 Differential Intersect Single-Update Fast Path

This implements a focused Materialize-style differential maintenance
optimization for `DifferentialIntersect.Apply`. A batch containing exactly one
non-zero update now bypasses the temporary validation overlay and the general
multi-row scan. The existing multi-row path remains unchanged.

## Correctness

The fast path preserves the general path's behavior:

- required-key validation happens before mutation;
- negative multiplicities and count overflow are rejected atomically;
- left-side payload retention is unchanged;
- output timestamps and signed transitions are unchanged;
- zero-diff updates remain no-ops;
- multi-row batches still use the staged validation overlay.

Regression coverage is in
`hat/hatSql/m038_differential_intersect_single_update_test.go`.

## Benchmark

Command: `make benchmark-differential-intersect`

Machine: AMD Ryzen 9 5950X, Linux amd64. Each sub-benchmark was sampled five
times by the repository benchmark script.

### Raw incremental samples

| Version | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | ---: | ---: |
| Before | 413675, 423777, 394034, 402657, 401650 | 657533, 657532, 657532, 657532, 657532 | 2837, 2837, 2837, 2837, 2837 |
| After | 370079, 368606, 372397, 381131, 382004 | 651388, 651388, 651388, 651388, 651389 | 2709, 2709, 2709, 2709, 2709 |

Median incremental cost improved from `402657` to `372397 ns/op`: `1.08x`
faster, `0.93%` fewer bytes, and `4.51%` fewer allocations.

### Full rebuild control samples

| Version | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | ---: | ---: |
| Before | 16801485, 15635852, 15735226, 15814870, 16061680 | 20361938, 20361824, 20361828, 20361830, 20361837 | 1800, 1799, 1799, 1799, 1799 |
| After | 16145280, 15917408, 15609382, 15885987, 15765660 | 20361911, 20361840, 20361830, 20361920, 20361832 | 1799, 1799, 1799, 1799, 1799 |

The rebuild control path is not changed by this feature; its samples remain in
the same range. The code path being optimized is only the single-update
incremental branch.

## Verification

- `make test-differential-intersect`
- `make verify-differential-intersect`
- `make benchmark-differential-intersect`
