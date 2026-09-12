# C212: Precomputed Join Probe Keys

This adopts the useful part of ClickHouse-style hot hash-probe optimization
for the SQL inner-hash-join executor.

## Implementation

- Numeric keys are normalized through the existing SQL numeric conversion and
  indexed by precomputed `float64` payload bits. Integer and floating-point
  values retain the previous equality behavior; NaN and signed zero are
  handled explicitly.
- Strings use the runtime's native `map[string][]int` path without rebuilding
  the old `string:` canonical key, so the original string bytes remain the
  collision-safe equality check.
- Booleans use two fixed slots.
- Duplicate matches remain in source order, unsupported values remain
  unmatchable, and the executor's deterministic output ordering is unchanged.
- The index is local to each hash join and is not retained between queries.

The optimization is applied to the existing reordered inner-hash-join paths.
Outer joins, cross joins, unsupported join shapes, and incomplete statistics
continue through their existing logic.

## Prefetch Decision

Portable Go does not expose a stable software-prefetch instruction. An
architecture-specific unsafe or assembly hook would add portability and
maintenance cost without a measured benefit on the supported build targets,
so no fake no-op hook was added. The adopted part of C212 is the allocation-free
typed probe representation and precomputed numeric lookup token; hardware
prefetch remains a future architecture-specific experiment.

## Verification

The red tests were added before the implementation and cover numeric, string,
boolean, NaN, signed-zero, unsupported-value, duplicate-row, and SQL executor
behavior. The focused package tests, full package tests, race detector, and vet
all pass.

The end-to-end benchmark uses the existing C211 three-way join workload
(500/200/20 unique numeric rows). Each row below is one benchmark sample; the
median is used for the comparison.

| Variant | Before ns/op | Final ns/op | Improvement | Before B/op | Final B/op | Before allocs/op | Final allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| No statistics, median | 294,074 | 263,311 | 1.12x | 467,839 | 446,780 | 3,214 | 2,476 |
| Statistics, median | 284,424 | 243,179 | 1.17x | 466,561 | 445,533 | 3,215 | 2,477 |

Raw end-to-end samples:

| Variant | Before ns/op samples | Final ns/op samples |
| --- | --- | --- |
| No statistics | 308,516; 291,540; 294,074; 293,200; 303,689 | 267,014; 269,617; 263,311; 251,030; 252,524 |
| Statistics | 284,424; 287,907; 279,665; 283,466; 285,881 | 243,179; 243,292; 245,708; 238,705; 234,074 |

The direct index benchmark compares the old canonical-string map with the new
typed index in the same process:

| Key type | Baseline ns/op | New ns/op | Improvement | Baseline B/op | New B/op | Baseline allocs/op | New allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Numeric, median | 49,620 | 20,257 | 2.45x | 31,880 | 20,568 | 771 | 260 |
| String, median | 29,619 | 21,460 | 1.38x | 32,040 | 23,896 | 771 | 260 |

`B/op` is cumulative benchmark allocation, not resident RSS. The workload
shows approximately 4.5% lower end-to-end cumulative bytes and 23% fewer
allocations, with no new process-global or cross-query retained structure.
