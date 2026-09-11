# SQL ARGMAX and ARGMIN

`ARGMAX(payload, ordering_value)` returns the payload from the row with the
greatest non-NULL ordering value. `ARGMIN` returns the payload from the row
with the smallest non-NULL ordering value.

Both functions require exactly two arguments and can be used as ordinary
aggregates, grouped aggregates, filtered aggregates, or window aggregates.
The payload and ordering expressions are evaluated for each row. Rows where
either expression is `NULL` are ignored. Ties keep the first row encountered
by the query's input or window order. String ordering follows the active SQL
collation.

## Examples

Given these rows:

| region | service | status | observed_at |
|---|---|---|---:|
| ap-southeast | api | degraded | 100 |
| ap-southeast | api | healthy | 200 |
| ap-southeast | api | healthy | 200 |
| eu-west | api | healthy | 150 |

The latest status per region can be selected with:

```sql
SELECT region, ARGMAX(status, observed_at) AS latest_status
FROM CACHE('service_events')
GROUP BY region
ORDER BY region;
```

The result is:

| region | latest_status |
|---|---|
| ap-southeast | healthy |
| eu-west | healthy |

The first `healthy` row wins the equal timestamp tie. To return both the
payload at the maximum and minimum values in one scan:

```sql
SELECT ARGMAX(service, observed_at) AS latest_service,
       ARGMIN(service, observed_at) AS earliest_service
FROM CACHE('service_events');
```

`FILTER` and window frames retain the normal SQL evaluator behavior:

```sql
SELECT ARGMAX(status, observed_at) FILTER (WHERE region = 'ap-southeast')
       AS latest_ap_status
FROM CACHE('service_events');
```

## Execution

Global queries containing only direct field or literal arguments use a
constant-state stream. A simple field-versus-literal `WHERE` predicate is
applied while rows are scanned, so the executor does not materialize an
intermediate row map for that shape. Complex expressions, grouped queries,
filters, active result-byte limits, and unsupported source resolvers use the
existing general evaluator and preserve the same result semantics.

The stream state is bounded by the number of aggregate expressions, not by
the number of input rows. No index or persistent metadata is added, and the
optimization is automatic for eligible queries.

## Measured Tradeoff

The focused benchmark uses 10,000 deterministic rows and five benchmark
samples per case on an AMD Ryzen 9 5950X. The control runs two ordered
`LIMIT 1` queries to obtain the same maximum and minimum payloads. The
`ARGMAX`/`ARGMIN` query returns both values in one SQL query, so the result is
most useful as a replacement for that common workaround rather than as a
claim about a new general SQL engine.

| Workload | Median time | Bytes/op | Allocs/op | Time improvement | Allocation-volume improvement | Allocation-count improvement |
|---|---:|---:|---:|---:|---:|---:|
| Global 10,000-row scan | 1,028,248 ns | 5,816 | 25 | 1.29x | 1.43x | 1.84x |
| Filtered 10,000-row scan | 1,155,990 ns | 6,984 | 31 | 2.21x | 1.50x | 1.74x |

The comparison is against two sort-and-limit queries. The benchmark includes
the raw five-run output in [BENCHMARK.md](BENCHMARK.md#sql-argmax-and-argmin).
