# CH-002 Physical Part And Mark Pruning

This is an opt-in ClickHouse-style sparse primary-key optimization for
columnar SQL sources.

## API

Implement `hatSql.ColumnarPartsSourceResolver` alongside the existing
`ColumnarSourceResolver`:

```go
type ColumnarSourcePart struct {
    Batch    ColumnarBatch
    Segments *ColumnarNumericSegments
}

type ColumnarPartsSourceResolver interface {
    BorrowSQLColumnarSourceParts(name, key string, fields []string) ([]ColumnarSourcePart, bool, error)
}
```

`Segments.SparsePrimaryField` or the composite sparse-primary fields must be
ordered and complete for the part. The executor validates the bounds before
using them. Missing, malformed, unordered, NaN, or shape-mismatched metadata
retains the part rather than risking a false negative.

## Execution policy

1. A literal numeric `WHERE` conjunction is converted to the existing sparse
   primary mark range logic.
2. Parts whose validated bounds cannot overlap the predicate are discarded
   before the columnar scan.
3. Zero surviving parts returns an empty columnar batch.
4. One surviving part is returned directly without copying or merging it.
5. Two or more surviving parts use the existing source resolver path. This is
   deliberate: materializing a temporary merged batch was slower and much more
   memory-heavy than the baseline.

The feature is disabled unless a source implements the new resolver. Existing
resolvers and query semantics are unchanged.

## Benchmark

Machine: AMD Ryzen 9 5950X 16-Core Processor, linux/amd64. Workload: 64
ordered parts, 1,024 rows per part, numeric `id` primary bounds.

Raw `make benchmark-ch002-physical-part-pruning` samples (`ns/op`, `B/op`,
`allocs/op`):

| Path | Samples | Median | Memory |
| --- | --- | ---: | ---: |
| Baseline, scan all parts | 49,016; 45,361; 44,351; 49,459; 49,581 | 49,016 | 0 B, 0 allocs |
| Single-part selection and scan | 2,535; 2,360; 2,611; 2,775; 2,611 | 2,611 | 0 B, 0 allocs |
| Diagnostic multi-part prune and scan | 16,525; 17,747; 16,759; 17,294; 17,705 | 17,294 | 6,784 B, 1 alloc |

The production single-part path is approximately **18.8x faster** in this
point-range workload with no additional allocation. The diagnostic multi-part
helper is not used by the executor; multiple candidates deliberately fall
back to the existing resolver.

### Rejected design

An initial implementation merged all selected parts into one temporary batch.
Its raw samples were 414,895; 412,057; 427,983; 426,909; and 419,053 ns/op,
with 638,123 B/op and 52 allocs/op. That is approximately **8.5x slower** and
far more memory-intensive than the baseline, so it was removed before commit.

## Verification

```text
make format-ch002-physical-part-pruning
make test-ch002-physical-part-pruning
make benchmark-ch002-physical-part-pruning
make race-ch002-physical-part-pruning
make vet-ch002-physical-part-pruning
```

The current scope provides the planner/executor contract and safety checks.
Storage adapters still own physical part creation, persistence, and loading.
