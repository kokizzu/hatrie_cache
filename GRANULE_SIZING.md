# Adaptive Granule Sizing

`hat/hatSql.GranuleSizingPolicy` converts observed predicate selectivity into a
bounded suggestion for a future columnar scan granule. It is a policy helper;
the current query path is unchanged unless the caller applies the result.

```go
policy, err := hatSql.NewGranuleSizingPolicy(hatSql.GranuleSizingOptions{
	MinRows:           256,
	DefaultRows:       8192,
	MaxRows:           65536,
	TargetSelectivity: 0.10,
})
nextRows := policy.Suggest(currentRows, scannedRows, matchedRows)
```

When fields are zero, the defaults are 256, 8192, 65536, and 10 percent.
Selective observations reduce the next granule to limit read amplification;
dense observations increase it to improve throughput. Results are clamped to
the configured bounds. Empty or inconsistent observations preserve the
current bounded size, and zero matches select the minimum.

The policy has no retained state and performs no allocations during
`Suggest`. Callers can persist observations per predicate or part and choose
when to apply a new size during future reads.
