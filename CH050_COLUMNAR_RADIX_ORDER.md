# CH-050: Typed Columnar Radix Order

## Idea

ClickHouse favors typed, column-oriented execution for order and limit
operations. The existing columnar order cache used a generic value wrapper and
`sort.Slice` even when every value in a large order column was an `int64`.

The cache now detects homogeneous `int64` columns with at least 256 rows and
builds the ordinal projection with a stable eight-pass radix sort. Signed
values are transformed by flipping the sign bit, so the byte-wise order is
identical to signed numeric order. Equal values retain input order.

The optimization is deliberately narrow: small inputs, mixed numeric types,
strings, nulls, and unavailable fields continue through the existing generic
path. No public API or cache configuration changes.

## Measurement

`BenchmarkRound14TypedTableColumnarInt64Order` sorts 100,000 rows and was run
five times before and after the change on the same machine.

| Metric | Before median | After median | Improvement |
| --- | ---: | ---: | ---: |
| CPU | 15,165,153 ns/op | 4,684,648 ns/op | 3.24x faster |
| Heap | 3,604,610 B/op | 1,605,658 B/op | 2.24x lower |
| Allocations | 7 allocs/op | 3 allocs/op | 2.33x fewer |

The raw samples are in [BENCHMARK.md](BENCHMARK.md). The threshold and strict
`int64` admission keep the generic fallback available when the radix path is
not expected to win or could change type semantics.
