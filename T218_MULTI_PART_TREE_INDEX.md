# T218: Multi-Part TREE-Style Ordered Index

`hat/hatDataStructure.MultiPartTreeIndex[T, K]` is an opt-in ordered index for
workloads that publish sorted batches and read ordered prefixes or ranges. It
uses compact immutable sorted-vector parts rather than pointer-heavy tree
nodes. The name is TREE-style because it provides ordered-tree operations and
range semantics; the physical representation is deliberately run-oriented.

## Behavior

- `AddPart` copies and sorts one non-empty immutable part, then publishes it in
  one generation change.
- `Prefix` and `Range` binary-search every part and merge the matching cursors
  with a small k-way heap.
- Equal keys are deterministic: lower entry IDs precede higher IDs, then lower
  part IDs break any remaining tie.
- `DeletePart`, `Clear`, and `AddPart` invalidate existing iterators.
- The comparator is supplied by the caller, so composite keys can model an
  equality prefix followed by an ordered suffix.

The primitive does not change the default `OrderedIndex`, SQL planner,
persistence, replication, or backup behavior. Callers own part lifecycle and
may use `DeletePart` after a higher-level merge or retention decision.

```go
index, err := hatDataStructure.NewMultiPartTreeIndex[int, compositeKey](compare)
partID, err := index.AddPart([]hatDataStructure.TreeIndexEntry[int, compositeKey]{
    {ID: 1, Key: compositeKey{Region: "apac", Sequence: 1}, Value: 11},
})
iterator, ok := index.Prefix(
    compositeKey{Region: "apac", Sequence: minSequence},
    compositeKey{Region: "apac", Sequence: maxSequence},
)
```

The caller must use bounds that are valid for its comparator. `Prefix` is an
ordered-bound convenience, not a string-prefix parser; for a composite key,
pass the smallest and largest suffix values for the desired prefix.

## Measurement

Command: `make benchmark-t218` on an AMD Ryzen 9 5950X, Go benchmark mode,
three samples, 16 parts of 1,024 rows each. The mutable comparison repeatedly
calls `OrderedIndex.Upsert` for the same batches. The read comparison scans
the same group prefix and returns the same logical rows.

| Workload | Multi-part index | Single `OrderedIndex` | Relative result |
| --- | ---: | ---: | --- |
| Publish/build 16,384 rows | 5,993,274 ns/op; 405,600 B/op; 88 allocs/op | 1,205,656,171 ns/op; 1,639,824 B/op; 84 allocs/op | 201.1x faster; 4.0x lower build bytes; 1.05x more allocation events |
| Prefix scan | 6,375 ns/op; 1,744 B/op; 6 allocs/op | 1,274 ns/op; 0 B/op; 0 allocs/op | single-vector scan is 5.0x faster |

Raw samples:

```text
BenchmarkT218MultiPartPublish-32             42    6390143 ns/op    405818 B/op  88 allocs/op
BenchmarkT218MultiPartPublish-32             43    5993274 ns/op    405600 B/op  88 allocs/op
BenchmarkT218MultiPartPublish-32             43    5714983 ns/op    405606 B/op  88 allocs/op
BenchmarkT218SingleOrderedIndexBuild-32       1 1205656171 ns/op   1639824 B/op  84 allocs/op
BenchmarkT218SingleOrderedIndexBuild-32       1 1286112213 ns/op   1639824 B/op  84 allocs/op
BenchmarkT218SingleOrderedIndexBuild-32       1 1172476456 ns/op   1639824 B/op  84 allocs/op
BenchmarkT218MultiPartPrefixScan-32       37030       6774 ns/op       1744 B/op   6 allocs/op
BenchmarkT218MultiPartPrefixScan-32       35131       6335 ns/op       1744 B/op   6 allocs/op
BenchmarkT218MultiPartPrefixScan-32       37816       6375 ns/op       1744 B/op   6 allocs/op
BenchmarkT218SingleOrderedPrefixScan-32  200220       1274 ns/op          0 B/op   0 allocs/op
BenchmarkT218SingleOrderedPrefixScan-32  201796       1169 ns/op          0 B/op   0 allocs/op
BenchmarkT218SingleOrderedPrefixScan-32  164108       1523 ns/op          0 B/op   0 allocs/op
```

This is not a universal replacement for `OrderedIndex`: the merged read path
has a real CPU and allocation cost. It is useful when batch publication,
independent part retention, or avoiding repeated global insertion dominates the
workload. It remains opt-in until a caller has measured that workload shape.
