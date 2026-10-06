# C236: Data-Skipping Explain Output

C236 was already present in the current SQL executor through the C022 explain
pruning and CH024 skip-index usefulness work. This reconciliation records the
capability in the round-two catalog; it does not change query results or add a
new runtime flag.

## Explain Surface

`EXPLAIN ANALYZE` exposes machine-readable pruning information on each affected
`ExplainStep`:

- `ExplainStep.Index` contains equality-index diagnostics such as candidate and
  skipped rows/segments.
- `ExplainStep.Pruning` contains `total_rows`, `skipped_rows`, `scanned_rows`,
  `matched_rows`, `residual_rows`, and
  `residual_false_positive_rate`.
- The tabular result exposes the same counters as columns, so operators can
  inspect them without decoding the JSON plan.

The existing node names identify the physical decision:

- `COLUMNAR BLOOM SEGMENT SKIP`
- `COLUMNAR NGRAM SEGMENT SKIP`
- `COLUMNAR NUMERIC SEGMENT SKIP`
- `COLUMNAR SEGMENT SKIP`
- `COLUMNAR PRIMARY MARK SKIP`

`skipped_rows` are rejected by conservative metadata before exact predicate
evaluation. `residual_rows` are admitted by the metadata but rejected by the
exact predicate. That distinction makes both useful pruning and weak or
over-broad marks visible. Invalid or missing metadata remains conservative and
does not change SQL NULL, NaN, or type-conversion semantics.

The feature is explain-only. Ordinary queries retain the existing execution
and result shape, and the executor does not retain unbounded diagnostic state.

## Verification

The existing focused targets cover exact query results, numeric segment and
primary-mark reporting, JSON plan fields, the streaming callback path, and
race behavior:

```text
make test-ch022-explain-pruning
make test-ch022-race
```

The source documentation remains in
[`COLUMNAR_RANGE_SKIPPING.md`](COLUMNAR_RANGE_SKIPPING.md) and
[`CH024_SKIP_INDEX_USEFULNESS.md`](CH024_SKIP_INDEX_USEFULNESS.md).

## Current Measurements

The measurements below were rerun on an AMD Ryzen 9 5950X, Linux/amd64. They
are evidence for the existing implementation, not a before/after claim for
this documentation-only reconciliation.

`make benchmark-ch022-explain-pruning`:

| Benchmark | Raw ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| `EXPLAIN ANALYZE` | 17423, 17418, 16976, 17142, 17710 | 17418 | 15868-15870 | 124 |
| plan wire bytes | 1230, 1263, 1284, 1238, 1249 | 1249 | 505 | 2 |

The structured pruning payload adds 129 bytes to the legacy plan in the wire
benchmark.

The existing CH024 before/after harness was also rerun:

| Harness | Raw ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| before | 23394, 21933, 22661, 22598, 20897 | 22598 | 16579-16583 | 141 |
| after | 25742, 22757, 25327, 20768, 25074 | 25074 | 16578-16586 | 141 |

That noisy pair is about 1.11x slower in the after median, with unchanged
allocation count and effectively unchanged bytes. It is not attributed to this
commit because no executor code changed here; the raw result is retained so a
future optimization can use the same workload and reject regressions.
