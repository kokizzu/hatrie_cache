# Mergeable SQL t-digest Percentile States

Status: implemented as an opt-in SQL state/merge pair.

```sql
APPROX_TDIGEST_PERCENTILE_STATE(value[, compression])
APPROX_TDIGEST_PERCENTILE_MERGE(state, quantile)
```

The state function builds the existing bounded `hatDataStructure.TDigest` and
returns its checksummed `HAG1` aggregate state. The merge function accepts one
or more such states and evaluates any requested quantile. A single stored state
can therefore serve p50, p95, and p99 queries without replaying the original
rows. This follows the same partial-state direction as ClickHouse's
quantile-state functions: [ClickHouse quantile aggregate documentation](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantile).

## Semantics

- `compression` defaults to `DefaultTDigestCompression` and must be within the
  existing t-digest bounds.
- `quantile` must be in `[0, 1]`.
- `NULL`, NaN, and infinities are ignored while producing a state.
- States with different compression values are rejected during merge.
- Malformed, corrupt, wrong-kind, and non-`[]byte` states are rejected.
- An empty state is valid HAG1 data; merging only empty states returns `NULL`.
- The direct no-group SQL stream path is supported, including the existing
  aggregate `FILTER` handling. The feature does not change the existing
  materialized percentile function.

## Measurement

The dedicated benchmark uses 10,000 numeric rows and five samples. Values are
medians; lower is better.

| Path | ns/op | B/op | allocs/op | wire bytes/op | CPU vs materialized | Memory vs materialized |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing `APPROX_TDIGEST_PERCENTILE` | 5,131,224 | 50,630 | 166 | - | 1.00x | 1.00x |
| `APPROX_TDIGEST_PERCENTILE_STATE` | 5,144,576 | 76,008 | 176 | 11,012 | 1.00x | 1.50x |
| `APPROX_TDIGEST_PERCENTILE_MERGE` (two 5,000-row states) | 191,182 | 128,704 | 46 | - | separate merge workload | separate merge workload |

State production is CPU-neutral within benchmark noise and adds the expected
serialization allocation. The extra bytes are the actual reusable wire state,
not retained per-row state. Because callers opt in when they need persistence,
transport, or repeated quantiles, the existing percentile default remains
unchanged. Merge work is bounded by the compressed centroid state rather than
the original input rows.

Reproduce with:

```text
make test-chu43
make test-chu43-package
make race-chu43
make vet-chu43
make benchmark-chu43
```
