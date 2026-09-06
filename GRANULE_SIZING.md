# Adaptive Columnar Granule Sizing

`hatSql.GranuleSizingPolicy` suggests a bounded next scan granule from observed
predicate selectivity. Selective predicates shrink later granules; dense
predicates grow them toward the configured maximum.

```go
policy, err := hatSql.NewGranuleSizingPolicy(hatSql.GranuleSizingOptions{
    MinRows:           256,
    DefaultRows:       8192,
    MaxRows:           65536,
    TargetSelectivity: 0.10,
})
next := policy.Suggest(currentRows, scannedRows, matchedRows)
```

The suggestion is based on `currentRows * observedSelectivity /
TargetSelectivity`, then clamped to `[MinRows, MaxRows]`. Zero matches choose
the minimum. A zero or inconsistent observation retains the current bounded
size. The policy is immutable and retains no history, so callers can keep one
instance per layout or workload and decide when to apply suggestions.

Zero options use conservative defaults: 256 minimum rows, 8,192 default rows,
65,536 maximum rows, and 10% target selectivity. Negative values, invalid
ordering, and non-finite or out-of-range selectivity are rejected. The method
does not change query results; it only changes how much data a future scan
attempts per granule.

The policy is intentionally advisory. Storage adapters still own mark
boundaries, cancellation, memory budgets, and persistence. It should be
combined with actual segment statistics and reset or recreated when the data
distribution changes materially.

Focused coverage is in `hat/hatSql/granule_sizing_test.go`, including selective,
dense, zero-match, invalid-observation, and boundary behavior plus an
allocation-reporting benchmark.
