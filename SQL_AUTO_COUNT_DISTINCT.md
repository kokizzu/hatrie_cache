# Automatic Distinct Counting

`AUTO_COUNT_DISTINCT` keeps a small cardinality exact and promotes to the
existing HyperLogLog sketch once the configured distinct-value threshold is
crossed.

```sql
SELECT region,
       AUTO_COUNT_DISTINCT(visitor) AS unique_visitors
FROM CACHE('events')
GROUP BY region;
```

The full form is:

```sql
AUTO_COUNT_DISTINCT(value, exact_limit, precision)
```

`exact_limit` defaults to `1024` and is bounded to `0..65536`. `precision`
defaults to the existing HyperLogLog precision and must be between `4` and
`20`. A zero exact limit selects HyperLogLog immediately. Values are counted
exactly while the distinct set is at or below the limit; the next distinct
value promotes the state and the result becomes an estimate. NULL values are
ignored, matching `APPROX_COUNT_DISTINCT`.

The function works in both global streaming aggregates and grouped queries,
and supports `FILTER`. It returns `uint64`, like the existing approximate
distinct aggregate. Invalid options fail before query execution.

## Measurement

The benchmark uses 10,000 rows. The low-cardinality workload has 64 distinct
visitors; the high-cardinality workload has 10,000. It ran on Linux/amd64
with an AMD Ryzen 9 5950X, five samples, and `-benchmem`.

| Workload | Median ns/op | B/op | Allocs/op | Improvement / cost |
| --- | ---: | ---: | ---: | --- |
| `APPROX_COUNT_DISTINCT`, low cardinality | 1,493,541 | 341,238 | 20,027 | control |
| `AUTO_COUNT_DISTINCT`, exact low cardinality | 1,568,518 | 332,044 | 20,037 | exact result, 5.0% slower, 2.7% lower B/op |
| `APPROX_COUNT_DISTINCT`, high cardinality | 1,662,580 | 341,247 | 20,028 | control |
| `AUTO_COUNT_DISTINCT`, promoted HLL | 1,677,995 | 355,059 | 20,042 | 0.9% slower, 4.0% higher B/op |

This is an accuracy and bounded-state feature, not a universal throughput
optimization. The high-cardinality cost is the temporary exact map and HLL
promotion; callers who only need an estimate should continue using
`APPROX_COUNT_DISTINCT`.

## Verification

```text
make test-ch039
make benchmark-ch039-after
```
