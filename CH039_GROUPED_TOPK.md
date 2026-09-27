# CH-039 Grouped Approximate Top-K

This is the ClickHouse-inspired completion of the grouped approximate
aggregate path. `APPROX_TOP_K(value[, capacity])` now uses the native grouped
dataflow executor when its value expression is a scalar field or literal.

## Implementation

- Native groups own one bounded Space-Saving accumulator each.
- The accumulator is shared with the materialized evaluator, preserving the
  existing estimate, error, NULL, tie ordering, and capacity semantics.
- Group creation clones a fresh accumulator, so values cannot leak between
  groups or composite group keys.
- Unsupported expressions and filtered or dynamic shapes keep the existing
  materialized fallback.

## Benchmark

Run with:

```text
make benchmark-ch039-grouped-topk
```

The workload contains 20,000 rows, 64 string groups, five possible state
values, and `APPROX_TOP_K(state, 8)`. Results are five samples on an AMD Ryzen
9 5950X; medians are shown and lower is better.

| Path | ns/op | B/op | allocs/op | CPU vs original | Memory vs original | Allocs vs original |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original materialized evaluator | 13,653,938 | 20,540,414 | 121,207 | 1.00x | 1.00x | 1.00x |
| Shared accumulator, materialized fallback | 13,508,762 | 20,556,807 | 121,335 | 1.01x | 1.001x | 1.001x |
| Native grouped dataflow | 5,950,751 | 4,235,438 | 60,949 | 2.29x | 4.85x | 1.99x |

The fallback refactor adds approximately 0.08% measured bytes and 0.11%
allocations in this workload. The native path is approximately 2.29x faster
with 4.85x less measured allocation and 1.99x fewer allocations.

Raw samples are `ns/op B/op allocs/op`:

```text
Original materialized evaluator:
14428221 20540424 121207
14723545 20540378 121207
13321994 20540409 121207
13465253 20540449 121207
13653938 20540414 121207

Shared accumulator, materialized fallback:
13764651 20556826 121335
12892823 20556745 121335
13745995 20556640 121335
13265246 20556807 121335
13508762 20556808 121335

Native grouped dataflow:
5950751 4235466 60949
5938327 4235485 60949
6108471 4235407 60949
6193685 4235353 60949
5586062 4235438 60949
```
