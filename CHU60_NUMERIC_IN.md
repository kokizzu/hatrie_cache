# CHU60: Columnar Numeric IN

## Scope

Direct numeric literal `field IN (...)` predicates now use a compact sorted
membership set. Duplicate literals are removed once, packed `int64` and
`float64` columns are checked without boxing through `ColumnarBatch.Value`, and
the existing columnar projection, offset, limit, and cancellation boundaries
remain in force.

The admission rule is deliberately narrow:

- only direct `IN` on one source field is admitted;
- every list element must be a finite, non-NULL numeric literal;
- `NOT IN`, NULL-containing lists, dynamic expressions, mixed string/numeric
  lists, and wider conjunctions retain the existing evaluator;
- the set is a query-local sorted slice, so no persistent index or storage
  format changes.

## Measurement

Commands:

```sh
make benchmark-chu60-numeric-in
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has 16,384 packed
`int64` rows with values `row % 4096`; the query contains five matching
literal values.

| Workload | Before median | After median | Improvement | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Literal numeric `IN` | 10,567,747 ns | 140,039 ns | 75.46x faster | 15,342,184 | 13,560 | 97,355 | 89 |
| Equivalent equality `OR` control | 12,102,165 ns | 12,460,248 ns | control | 17,283,223 | 17,283,242 | 76,912 | 76,912 |

The measured `IN` path uses 1,131.43x less allocated heap and 1,093.88x fewer
allocations than its pre-change generic execution. The result is workload
specific; large, dynamic, NULL-containing, and nonnumeric lists intentionally
fall back.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#chu60-numeric-in).
Correctness, race, vet, and broader relevant SQL checks are:

```sh
make test-chu60-numeric-in
make race-chu60-numeric-in
make vet-chu60-numeric-in
make verify-chu60-numeric-in
```
