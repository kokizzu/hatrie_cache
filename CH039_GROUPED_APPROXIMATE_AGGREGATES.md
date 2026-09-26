# CH-039 Grouped Approximate Aggregates

ClickHouse-style approximate aggregation is useful inside grouped queries, not
only for one global result. This change routes supported grouped
`APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE` expressions through the native
dataflow executor while keeping the existing HyperLogLog and quantile-sketch
algorithms and fallback semantics.

## Implementation

- Native aggregate planning accepts direct scalar arguments for
  `APPROX_COUNT_DISTINCT` and `APPROX_PERCENTILE`.
- Each native group receives a fresh sketch state. The clone is deliberate:
  native plans are reused as a template, and sharing a pointer-backed sketch
  would combine values from different groups.
- The per-group state isolation applies to the single-field, composite,
  triple-field, and quad-field grouped executors.
- Unsupported shapes continue to use the materialized evaluator. This feature
  does not change approximate algorithms, precision, epsilon, NULL handling,
  or error behavior.

## Supported Scope

Native grouped execution currently covers the existing eligible single-source
dataflow shapes with a scalar field or literal as the sketch input. State and
merge functions, `APPROX_TOP_K`, `AUTO_COUNT_DISTINCT`, and T-Digest variants
remain on their existing paths until separately benchmarked.

## Verification

The regression test compares two groups containing overlapping visitor values
and different latency distributions against the forced materialized fallback.
It also requires the automatic query observer to report `NATIVE DATAFLOW`.

```text
make test-ch039-grouped-approx
make race-ch039-grouped-approx
make benchmark-ch039-grouped-approx
```

The benchmark uses 20,000 rows, 64 string groups, HyperLogLog distinct counts,
and p95 quantile sketches. Median results and raw samples are recorded in
`BENCHMARK.md`.
