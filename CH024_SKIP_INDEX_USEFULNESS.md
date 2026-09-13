# CH-24: Skip-Index Usefulness Telemetry

ClickHouse-inspired pruning diagnostics now make the existing columnar
segment and primary-mark skips measurable in `EXPLAIN ANALYZE`. The query
executor still uses the same coarse metadata checks and exact predicate
evaluation; only the explain detail is richer.

## Reported Nodes

The counters are attached to these existing plan nodes when they skip rows:

- `COLUMNAR BLOOM SEGMENT SKIP`
- `COLUMNAR NGRAM SEGMENT SKIP`
- `COLUMNAR NUMERIC SEGMENT SKIP`
- `COLUMNAR SEGMENT SKIP`
- `COLUMNAR PRIMARY MARK SKIP`

Example detail:

```text
event.id >= 100 skipped_rows=2 scanned_rows=2 matched_rows=1 residual_rows=1 residual_false_positive_rate=50.00%
```

The values mean:

| Counter | Meaning |
| --- | --- |
| `skipped_rows` | Rows excluded by the coarse segment or mark metadata and never sent to the exact predicate. |
| `scanned_rows` | Rows admitted by the coarse metadata and evaluated by the exact predicate. |
| `matched_rows` | Rows that passed the exact predicate. |
| `residual_rows` | `scanned_rows - matched_rows`; rows admitted by the coarse index but rejected by the exact predicate. |
| `residual_false_positive_rate` | `residual_rows / scanned_rows * 100`, or `0.00%` when no rows were admitted. |

This is a row-level residual usefulness measure. It is intentionally named
`residual_false_positive_rate`: it is not a probabilistic Bloom-filter rate
and does not claim that every admitted row belongs to a distinct false-positive
segment. A high value means the coarse metadata is admitting much more data
than the exact predicate returns. A high `skipped_rows` value with a low
residual rate indicates useful pruning.

The detail is available through the normal `EXPLAIN ANALYZE` plan rows and
JSON plan output. Ordinary queries without explain/observation metrics do not
format these counters. No result, storage, or wire semantics change.

## Measurement

The benchmark runs one four-row numeric columnar source with two segments. One
segment is proven disjoint, while the other admits two rows and the exact
predicate matches one. Five samples, `-benchtime=1000x`, AMD Ryzen 9 5950X:

| Operation | Before | After | Relative change |
| --- | ---: | ---: | ---: |
| `EXPLAIN ANALYZE` | 13,078 ns/op | 13,162 ns/op | 1.006x CPU, +0.64% |
| Heap allocation | 11,810 B/op | 11,947 B/op | 1.012x, +1.16% |
| Allocations | 104 allocs/op | 107 allocs/op | 1.029x, +2.88% |

Raw samples:

```text
before: 13611 11810 104
before: 13313 11811 104
before: 12817 11810 104
before: 13078 11810 104
before: 12984 11810 104
after:  14727 11947 107
after:  13012 11947 107
after:  14438 11947 107
after:  13162 11947 107
after:  13125 11948 107
```

The cost is confined to explain/telemetry requests and buys actionable index
usefulness evidence. Reproduce with:

```text
make benchmark-ch024-before
make benchmark-ch024-after
```
