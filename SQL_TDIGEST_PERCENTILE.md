# SQL t-digest percentile

`APPROX_TDIGEST_PERCENTILE(value, quantile [, compression])` provides an
opt-in percentile aggregate with t-digest-style weighted centroids. It keeps
more resolution near both tails than the existing GK-style
`APPROX_PERCENTILE` aggregate.

```sql
SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.99, 100) AS p99
FROM CACHE('events');
```

`quantile` is a number from `0` through `1`. `compression` is an integer from
`20` through `1000` and defaults to `100`. `NULL`, non-numeric, NaN, and
infinite values are ignored. An empty input returns `NULL`.

The SQL aggregate is available in grouped queries and supports `FILTER`.
The existing `APPROX_PERCENTILE` implementation remains the default and is
unchanged; choose the t-digest function when tail accuracy is more important
than minimum CPU and allocation cost.

The reusable state is exported from `hatDataStructure` as `TDigest`, with
validated snapshots and same-compression `Merge` support for callers that
need to transfer or combine partial states.

## Measured tradeoff

The paired benchmark used 10,000 rows with a 99% tail-heavy workload on
Linux/amd64 and five samples per sub-benchmark:

| Algorithm | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Existing GK `APPROX_PERCENTILE` | 2,935,121 | 17,528 | 36 |
| t-digest `APPROX_TDIGEST_PERCENTILE` | 3,946,271 | 35,200 | 38 |

The t-digest path is about `1.34x` slower and uses about `2.01x` allocation
bytes in this workload. It is therefore opt-in rather than a replacement.
Its benefit is accuracy at steep tails: in an adversarial p99.9 workload with
an exact answer of `1,000,000`, GK returned `10,900,000` while t-digest
returned `1,000,000`.
