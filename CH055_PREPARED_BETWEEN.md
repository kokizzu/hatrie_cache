# CH-055 Prepared Literal `BETWEEN` Bounds

This ClickHouse-inspired SQL execution improvement prepares literal `BETWEEN`
and `NOT BETWEEN` bounds during query binding. The row evaluator reuses those
two values instead of evaluating two literal expression nodes for every input
row.

Only both-literal bounds are prepared. Parameter values are prepared after
binding, while field references, function calls, and other dynamic bounds keep
the existing evaluator. Invalid literal error values also use the existing
fallback so their error timing is unchanged. The collation is still supplied
at evaluation time, and the prepared program stores only two interface values;
it does not duplicate the bound values.

## Correctness Coverage

`hat/hatSql/ch055_between_program_test.go` verifies:

- literal `BETWEEN` preparation and in-range/out-of-range results;
- `NOT BETWEEN` inversion;
- `NULL` left values;
- dynamic field bounds remaining on the fallback path;
- bound parameters being prepared;
- Unicode case-insensitive collation.

Commands:

```text
make test-ch055-between
make race-ch055-between
make vet-ch055-between
```

## Measurement

The benchmark directly evaluates one aliased numeric row predicate. Both
variants report zero allocations. The baseline subcase uses the evaluator
without a prepared program; the prepared subcase calls
`prepareSQLBetweenExpr` once before the timed loop.

| Scenario | Before | After | Improvement | Before memory | After memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| literal numeric `BETWEEN` | 95.12 ns/op | 69.62 ns/op | 1.37x faster | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |

Baseline subcase raw output from `make benchmark-ch055-between`:

```text
BenchmarkCH055LiteralBetween/baseline-32  13601598  98.94 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  13159528  94.55 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  13747852  98.49 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  12745404  94.15 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  11747546  95.69 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  13136896  98.46 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  13832829  84.14 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  14613968  100.0 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  12270958  85.85 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/baseline-32  14044575  94.29 ns/op  0 B/op  0 allocs/op
```

Prepared subcase raw output:

```text
BenchmarkCH055LiteralBetween/prepared-32  16055967  68.46 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  17176946  68.08 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  17621407  62.66 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  20154252  61.75 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  20180863  67.15 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  15302745  70.77 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  17139667  73.25 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  15189106  74.37 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  18442345  72.40 ns/op  0 B/op  0 allocs/op
BenchmarkCH055LiteralBetween/prepared-32  14676066  79.45 ns/op  0 B/op  0 allocs/op
```

This is a CPU-only win for the scalar evaluator. It does not claim a memory
reduction, and dynamic-bound queries are intentionally not claimed as faster.
