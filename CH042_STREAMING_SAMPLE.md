# Streaming Bernoulli Table Samples

## What changed

`ExecuteSQLQueryRows` can now execute `TABLESAMPLE BERNOULLI` without first
materializing the source or the sampled rows. The path is intentionally narrow:

- the source is an untyped direct `CACHE` source;
- the resolver implements `StreamSQLSource`;
- the sample mode is `BERNOULLI`;
- the query is otherwise compatible with the existing row-stream executor.

The implementation applies the same seeded per-row decision as the materialized
executor, then lets the existing stream executor apply `WHERE`, `LIMIT`,
`OFFSET`, and projection. This keeps sampling-before-filter behavior intact.
The source row budget and execution cancellation are checked for every source
row, including rows rejected by the sample.

## What remains materialized

`ExecuteSQLQuery`, `TABLESAMPLE RESERVOIR`, typed sources, joins, ordering,
grouping, distinct queries, and other unsupported shapes retain the existing
materialized path. This avoids changing typed-source validation and complex
operator semantics in the first iteration.

The streaming wrapper does not expose partition-pruning capabilities. A
partition skip would change the number and order of rows consumed by the seeded
sampler, which would change repeatable results.

## Measurement

Workload: 10,000 source rows, 10% Bernoulli sample, repeatable seed 7,
`ExecuteSQLQueryRows`, five `-benchmem` samples on Linux/amd64 with an AMD
Ryzen 9 5950X.

| Metric | Materialized baseline | Streaming | Improvement |
| --- | ---: | ---: | ---: |
| Time | 2,402,302 ns/op | 629,780 ns/op | 3.81x faster |
| Timed heap | 4,085,160 B/op | 438,756 B/op | 9.31x lower |
| Allocations | 26,051 allocs/op | 6,043 allocs/op | 4.31x fewer |

Reproduce with:

```sh
make benchmark-chg04-sample-stream-baseline
make benchmark-chg04-sample-stream
```

The focused correctness and package checks are:

```sh
make test-chg04-sample-stream
make test-chg04-sample-stream-package
make race-chg04-sample-stream
make vet-chg04-sample-stream
```
