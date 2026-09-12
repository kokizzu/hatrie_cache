# C211 Statistics-Driven Join Order

Hatrie SQL now accepts an optional `SourceCardinalityResolver` alongside the
existing `SourceResolver` contract:

```go
type SourceCardinalityResolver interface {
	SQLSourceCardinality(name, key string) (rows int, exact bool, available bool, err error)
}
```

`TypedTable` implements this contract with an O(1) active-row count, excluding
deleted patch-part rows. `CatalogResolver` forwards the optional contract to
its underlying source resolver.

For a connected query with at least two `INNER` equality joins, no top-level
`WHERE`, and no sampling, the executor chooses the smallest available source
first and then greedily adds the smallest connected source. Ties use original
SQL source order. Only the current joined rows and the next source are kept by
this path; unjoined source envelopes are not retained in a map for the whole
query.

The optimization is deliberately conservative:

- `LEFT`, `RIGHT`, `FULL`, and `CROSS` joins are unchanged.
- Non-equality or disconnected join graphs are unchanged.
- Missing, negative, or failed cardinality metadata falls back to the prior
  exact-materialization planner.
- Approximate counts are allowed as performance hints; source rows are still
  resolved and all original join predicates are evaluated.
- Output rows are restored to the deterministic nested-loop order of the
  original SQL source list, including duplicate-key rows.

## Benchmark

Command:

```text
make benchmark-c211-publish
```

Workload: three connected inner joins with 500, 200, and 20 rows, producing 20
rows. The baseline resolver exposes only `SourceResolver`; the statistics
resolver exposes the same rows plus exact cardinalities.

### Raw samples

| Variant | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before, baseline `ns/op` | 298459 | 294099 | 338398 | 314120 | 291391 | 298459 |
| Final, baseline `ns/op` | 325270 | 298739 | 288034 | 292715 | 288101 | 292715 |
| Final, statistics `ns/op` | 289087 | 287980 | 302057 | 282745 | 286750 | 287980 |

Final statistics-driven execution is about `1.02x` faster than the final
baseline median. Final `B/op` is `466,566` versus `467,855` for baseline,
about `0.28%` lower. The statistics path uses `3,215 allocs/op` versus
`3,214 allocs/op`, so it is not an allocation-count win for this small
workload. Its primary memory benefit is lower peak retention of unjoined
source envelopes, not lower cumulative allocation.

The focused correctness tests compare result rows with and without metadata,
including duplicate keys, and verify deterministic fallback when metadata is
incomplete. Race and package tests cover the same execution path.
