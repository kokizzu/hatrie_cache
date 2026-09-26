# CH-048 Packed BETWEEN Predicates

`BETWEEN` is inclusive on both bounds. The columnar executor now has a fast
path for the common literal forms below:

- binary-collated string fields backed by a dictionary column;
- literal numeric fields backed by packed `int64` or `float64` columns.

Dictionary ranges are evaluated once per dictionary value and retained as a
small `uint64` mask for up to 64 values, or a boolean mask for wider
dictionaries. Numeric ranges are lowered to the existing packed `>=` and `<=`
kernels. The row loop performs no interface boxing or allocation on either
fast path.

The following intentionally retain the general evaluator: `NOT BETWEEN`,
dynamic bounds, NULL bounds, malformed packed columns, and non-binary string
collations. This preserves SQL UNKNOWN behavior and avoids making a broad
proof about collation or expression evaluation.

## Verification

Focused tests cover dictionary and numeric recognizer validation, inclusive
bounds, packed and legacy data, nullable values, `NOT BETWEEN` fallback, and
end-to-end query materialization:

```text
make format-ch048-between
make test-ch048-between
```

## Benchmark

The workload scans 4,096 rows five times per case on Linux/amd64, AMD Ryzen 9
5950X. The benchmark constructs the matcher before the timed loop and reports
the per-row filter path only.

| Workload | Pre-change median | Fallback median | Fast-path median | Fast-path improvement |
| --- | ---: | ---: | ---: | ---: |
| Dictionary string | 884,034 ns/op, 589,828 B/op, 12,288 allocs/op | 993,766 ns/op, 589,829 B/op, 12,288 allocs/op | 32,924 ns/op, 0 B/op, 0 allocs/op | 30.18x vs paired fallback |
| Packed numeric | 851,629 ns/op, 555,015 B/op, 12,032 allocs/op | 907,378 ns/op, 555,015 B/op, 12,032 allocs/op | 89,820 ns/op, 0 B/op, 0 allocs/op | 10.10x vs paired fallback |

The pre-change values were captured before the recognizers and dispatch paths
were implemented. The paired fallback is run in the same post-change binary to
control for benchmark-process variation. No wire or storage format changes are
involved.

Raw output from `make benchmark-ch048-between`:

```text
Pre-change dictionary: 884034 873617 884316 903999 859108 ns/op; 589828 B/op; 12288 allocs/op
Pre-change numeric:    856951 842416 848690 851694 851629 ns/op; 555015 B/op; 12032 allocs/op

BenchmarkCH048BetweenFallback/dictionary_packed-32 1031666 ns/op 589854 B/op 12288 allocs/op
BenchmarkCH048BetweenFallback/dictionary_packed-32  993766 ns/op 589829 B/op 12288 allocs/op
BenchmarkCH048BetweenFallback/dictionary_packed-32 1024012 ns/op 589830 B/op 12288 allocs/op
BenchmarkCH048BetweenFallback/dictionary_packed-32  980248 ns/op 589830 B/op 12288 allocs/op
BenchmarkCH048BetweenFallback/dictionary_packed-32  977268 ns/op 589829 B/op 12288 allocs/op
BenchmarkCH048BetweenFallback/numeric_packed-32     966781 ns/op 555017 B/op 12032 allocs/op
BenchmarkCH048BetweenFallback/numeric_packed-32     955646 ns/op 555017 B/op 12032 allocs/op
BenchmarkCH048BetweenFallback/numeric_packed-32     907378 ns/op 555015 B/op 12032 allocs/op
BenchmarkCH048BetweenFallback/numeric_packed-32     904946 ns/op 555017 B/op 12032 allocs/op
BenchmarkCH048BetweenFallback/numeric_packed-32     898995 ns/op 555017 B/op 12032 allocs/op
BenchmarkCH048BetweenFastPath/dictionary_packed-32   33265 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/dictionary_packed-32   32166 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/dictionary_packed-32   32099 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/dictionary_packed-32   32924 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/dictionary_packed-32   38003 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/numeric_packed-32      88633 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/numeric_packed-32      89820 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/numeric_packed-32      88905 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/numeric_packed-32      91879 ns/op      0 B/op     0 allocs/op
BenchmarkCH048BetweenFastPath/numeric_packed-32      91289 ns/op      0 B/op     0 allocs/op
```
