# CH-053 Prepared Literal `IN` Sets

This is a ClickHouse-inspired prepared-set optimization for SQL expression
evaluation. When every right-hand operand of an `IN` or `NOT IN` expression is
a literal, the bound expression retains one immutable value slice and reuses
it for every row. The evaluator no longer allocates a candidate slice per
row.

## Scope

- Literal `IN` and `NOT IN` values are prepared after query/parameter binding.
- SQL `NULL` membership and three-valued logic continue to use the existing
  comparison implementation.
- Dynamic list operands keep the existing evaluation path.
- The comparison algorithm remains linear; this change removes repeated
  materialization without changing mixed-type or collation semantics.
- The retained cost is one value slice per prepared expression. There is no
  global set cache and no storage or wire-format change.

## Measurement

Samples were collected on Linux `amd64`, Go benchmark workers `-32`, an AMD
Ryzen 9 5950X, and five runs per case. Each case evaluates a bound predicate
against one row and reports CPU, heap bytes, and allocation count.

| Workload | Before ns/op | After ns/op | CPU improvement | Before B/op | After B/op | Byte improvement | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 numeric literals | 323.9 | 157.1 | 2.06x | 128 | 0 | 100% | 1 | 0 |
| 32 numeric literals | 1,161 | 456.0 | 2.55x | 512 | 0 | 100% | 1 | 0 |
| 8 string literals | 325.5 | 150.9 | 2.16x | 128 | 0 | 100% | 1 | 0 |
| Dynamic mixed list | 328.8 | 336.9 | 0.98x | 128 | 128 | 1.00x | 1 | 1 |

The dynamic-list median movement is treated as noise rather than a claimed
regression or speedup: bytes and allocations are unchanged, and the hot path
still evaluates dynamic operands row by row. Literal lists retain their
original comparison order, so the optimization does not change first-match
behavior or `NULL` propagation.

## Raw output

```text
# Before
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  3684662  325.6 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  3776544  320.6 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  3721588  322.9 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  3591859  323.9 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  3684954  329.7 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  919728  1150 ns/op  512 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  1000000  1167 ns/op  512 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  1000000  1161 ns/op  512 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  985777  1156 ns/op  512 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  1037320  1162 ns/op  512 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3620689  329.8 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3652056  328.1 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3694579  330.9 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3547590  328.8 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3643503  328.7 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  3607210  330.5 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  3608996  328.8 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  3590740  321.3 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  3607632  325.8 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  3737500  325.5 ns/op  128 B/op  1 allocs/op

# After
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  7755207  155.7 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  7680774  157.8 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  6781471  174.1 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  7888342  150.7 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_numeric-32  6988876  157.1 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  2329645  441.4 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  2538522  443.3 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  2566094  463.7 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  2590774  466.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_32_numeric-32  2339878  456.0 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3097351  351.9 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3619584  341.4 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3597930  335.1 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3494152  336.9 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/dynamic_mixed-32  3532813  334.4 ns/op  128 B/op  1 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  7869982  152.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  7853562  150.9 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  8525336  141.2 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  8402794  141.0 ns/op  0 B/op  0 allocs/op
BenchmarkCH053PreparedLiteralInEvaluation/literal_8_string-32  7701627  151.7 ns/op  0 B/op  0 allocs/op
```

## Verification

```text
make test-ch053-in-program
make benchmark-ch053-in-program
make format-ch053-in-program
make race-ch053-in-program
make vet-ch053-in-program
```
