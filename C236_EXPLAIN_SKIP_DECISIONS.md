# C236 Explain Data-Skipping Decisions

C236 extends the existing `EXPLAIN ANALYZE` pruning telemetry with bounded
mark-level decisions. This follows ClickHouse's explain-index direction: a
plan can show not only how many rows were skipped, but which marks were
rejected by a data-skipping index and why the remaining marks were scanned.

## Output

Pruned plan steps retain the existing row counters and may now include:

```json
{
  "total_rows": 6,
  "skipped_rows": 4,
  "scanned_rows": 2,
  "matched_rows": 2,
  "marks_examined": 3,
  "rejected_marks": 2,
  "decisions": [
    {"mark": 0, "row_start": 0, "row_count": 2, "action": "skip", "reason": "numeric_minmax_excludes"},
    {"mark": 1, "row_start": 2, "row_count": 2, "action": "skip", "reason": "numeric_minmax_excludes"},
    {"mark": 2, "row_start": 4, "row_count": 2, "action": "scan", "reason": "numeric_minmax_may_match"}
  ]
}
```

`rejected_marks` counts marks for which the index proved that no match was
possible, so it is the count of `action: "skip"` decisions. A `scan` decision
means the mark may contain a match and still requires the exact residual
predicate. Reasons identify the conservative proof: numeric min/max,
primary-mark range, Bloom, n-gram, dictionary, or Top-N bound.

At most 64 decisions are retained per pruning step. `marks_examined` and
`rejected_marks` remain complete; `decisions_truncated: true` indicates that
the sample list was capped. This keeps EXPLAIN output bounded on large
columnar sources and avoids turning diagnostics into a second full mark
index.

The fields are available in both the structured `SQLQueryResult.Plan` and the
row form returned by `EXPLAIN ANALYZE`. Existing plans without pruning retain
their previous output shape. Normal queries do not collect the trace; the
additional work is limited to instrumented EXPLAIN paths.

## Tradeoff

The controlled `BenchmarkCH024ExplainSegmentSkip` comparison used five 2-second
samples with `GOMAXPROCS=1` on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Raw samples (ns/op) | Median | Memory | Relative result |
| --- | --- | ---: | ---: | --- |
| C230 baseline | 26,626; 27,140; 27,600; 26,168; 27,779 | 27,140 | 17,026 B/op, 141 allocs/op | baseline |
| C236 decision trace | 27,621; 28,033; 29,538; 28,318; 30,518 | 28,318 | 17,546 B/op, 147 allocs/op | 1.04x CPU, 1.03x bytes, 1.04x allocs |

The trace adds a small EXPLAIN-only cost for useful mark diagnostics; it is not
collected on the normal execution path.

## Verification

```sh
make test-c236-explain
make format-c236-explain
make benchmark-c236-before-after
```
