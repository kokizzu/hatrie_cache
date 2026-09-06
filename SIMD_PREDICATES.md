# Batch Predicate Masks

`hatPredicate` provides portable word-parallel predicate kernels for common
numeric and string filters. A caller supplies a reusable `[]uint64` selection
mask, so filtering can be chained without allocating a result slice for every
predicate:

```go
mask := make([]uint64, hatPredicate.MaskWords(len(values)))
matches, err := hatPredicate.MatchInt64(
	mask,
	values,
	hatPredicate.Int64GreaterEqual,
	100,
)
```

Numeric comparisons include equality, inequality, and all signed order
comparisons. String comparisons include equality, inequality, prefix, suffix,
and substring matching. Every supplied mask word is cleared before use,
including words beyond the current batch length, and the returned count is the
number of set bits.

The implementation is allocation-free on the hot path and uses a compact
64-values-per-word representation that is suitable for later bitmap AND/OR
operations and architecture-specific SIMD lowering. It intentionally has no
assembly or CPU-feature dependency, so behavior is identical across supported
platforms.

Invalid operators and undersized masks return errors. Existing SQL execution
paths are unchanged; planners can opt into the package when a columnar batch
already has typed `int64` or `string` values.
