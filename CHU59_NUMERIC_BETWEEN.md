# CHU59: Columnar Numeric BETWEEN

## Scope

Literal numeric `field BETWEEN lower AND upper` predicates now use the
existing packed numeric columnar kernels. The inclusive predicate is lowered
to `field >= lower AND field <= upper`, so segment pruning, fixed-width loads,
nullable validity checks, projection, and `LIMIT` handling are reused without
adding a new storage format or changing defaults.

The optimization is deliberately narrow:

- `NOT BETWEEN` remains on the general evaluator.
- Dynamic bounds, nonnumeric literals, joins, subqueries, and unsupported
  query shapes remain on their existing paths.
- Reversed bounds return no rows, and NULL values remain excluded by SQL
  three-valued `WHERE` semantics.

## Measurement

Commands:

```sh
make benchmark-chu59-numeric-between
```

Five benchmark samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has
16,384 packed `int64` rows with values `row % 4096`; the query selects values
between 1,000 and 3,000.

| Workload | Before median | After median | Improvement | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Literal `BETWEEN` | 12,624,036 ns | 2,259,046 ns | 5.59x faster | 18,546,526 | 2,978,893 | 113,320 | 24,050 |
| Equivalent `>=` + `<=` control | 2,080,976 ns | 2,285,638 ns | control | 2,983,541 | 2,983,540 | 24,070 | 24,070 |

Against the pre-change `BETWEEN` path, allocated heap is 6.23x lower and
allocations are 4.71x fewer. The optimized path is 0.99x the latency of the
equivalent existing two-comparison control, with effectively identical memory
and allocation behavior. The comparison is workload-specific; generic or
dynamic `BETWEEN` expressions intentionally do not claim this result.

Raw five-run samples are retained in the commit history and reproduced by the
Makefile benchmark target. Correctness, race, and vet checks are:

```sh
make test-chu59-numeric-between
make race-chu59-numeric-between
make vet-chu59-numeric-between
```
