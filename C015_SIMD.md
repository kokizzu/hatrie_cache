# SIMD Predicate Kernels

`hatPredicate` exposes allocation-free selection-mask kernels for typed
columnar batches. `MatchInt64` now dispatches signed equality and inequality
predicates to an AVX2 kernel when the host supports AVX2. Other numeric
comparisons use the portable packed-word kernel, and all paths preserve the
existing mask and error contract.

```go
mask := make([]uint64, hatPredicate.MaskWords(len(values)))
matches, err := hatPredicate.MatchInt64(
	mask,
	values,
	hatPredicate.Int64Equal,
	42,
)
```

The dispatch is runtime-gated. Non-amd64 builds, amd64 CPUs without AVX2, and
predicates not covered by the AVX2 kernel use the portable implementation; no
instruction-set assumption is exposed to callers. The existing string
predicates remain on their allocation-free standard-library-backed path. A
separate word-at-a-time string loop was measured slower and was not retained.

## Measurement

Host: AMD Ryzen 9 5950X, linux/amd64, 100,000 `int64` values, equality
predicate, reusable mask, `-benchmem`, five samples.

| Path | Time | Allocations |
| --- | ---: | ---: |
| Before | 92.6-103.7 us/op | 0 B/op, 0 allocs/op |
| After (`MatchInt64`) | 39.0-42.0 us/op | 0 B/op, 0 allocs/op |

This is approximately a 2.4x speedup for the measured equality workload. The
path does not allocate; it writes each 64-value mask word once. The speedup is
workload and CPU dependent, so the portable fallback remains the correctness
baseline.

Validation includes all six signed comparisons, mask boundaries at 64 values,
tail clearing, invalid operators, short masks, an arm64 compile check, and the
package race test.
