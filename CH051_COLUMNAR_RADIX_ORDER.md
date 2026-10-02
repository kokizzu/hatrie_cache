# CH-051: Typed int64 columnar radix order

Typed-table columnar `ORDER BY` cache construction now has a narrow fast path
for large homogeneous `int64` columns. The path keeps the existing generic
ordering behavior as the fallback and only admits batches with at least 256
rows where every value in the requested field is an `int64`.

The fast path uses a stable eight-pass byte radix sort over row ordinals. It
flips the sign bit while sorting so signed `int64` values retain normal
ascending order, including negative values. Stable ties preserve the existing
row order, which keeps duplicate ordering deterministic.

Small batches, mixed numeric types, `NULL`, strings, and unsupported values do
not use the fast path. No public SQL syntax, configuration, wire format, or
storage format changed.

## Measurement

The measurements use the same isolated source harness, compiler, fixture, and
100,000-row `int64` batch on the current clean base and the candidate. Each
case is the median of five benchmark samples.

| Case | CPU | Heap bytes | Allocations | Result |
| --- | ---: | ---: | ---: | --- |
| Generic columnar order | 14,545,159 ns/op | 3,604,536 B/op | 4 allocs/op | Baseline |
| Typed `int64` radix order | 4,169,117 ns/op | 1,605,632 B/op | 3 allocs/op | 3.49x faster; 2.24x lower heap; 1 fewer allocation |

Raw five-sample output:

```text
Before: 14473737, 14554159, 14560398, 14522053, 14730709 ns/op; 3604536-3604537 B/op; 4 allocs/op
After:  4194657, 4102916, 4197940, 4117654, 4169117 ns/op; 1605632 B/op; 3 allocs/op
```

Focused signed-order, stable-tie, fallback, and large-input tests passed under
the temporary isolated harness with `go test`, `-race`, and `go vet`. The full
`hat/hatSql` package remains blocked on unrelated missing symbols already
present on the clean base (`MaxDataflowTextBytes`, `TypedTableDate`, and
`TypedTableTimestamp`); this feature does not mask or alter that issue.

The tradeoff is deliberately bounded: the radix path allocates one ordinal
buffer and one scratch buffer for eligible large batches, while the generic
path remains unchanged for all other shapes. The 256-row admission threshold
avoids paying radix setup cost for small orders.
