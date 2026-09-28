# M212 Logical Compaction

`hatDataStructure.LogicalCompaction[T]` retains differential records while
allowing a caller to advance a monotone frontier. `CompactThrough(frontier)`
folds every record at or before the frontier into one record per data value at
the frontier. Records after the frontier remain unchanged.

```go
state := hatDataStructure.NewLogicalCompaction[string]()
_ = state.Add("eu", 1, 2)
_ = state.Add("eu", 4, -1)
_ = state.Add("us", 2, 3)

removed, err := state.CompactThrough(10)
// removed == 3
// state.Records() contains eu@10:+1 and us@10:+3.
```

The structure is intentionally caller-synchronized, like
`DifferentialMultiset`. `Since` reports the current lower frontier and `Len`
reports retained nonzero physical records. After compaction, an `Add` with a
timestamp before `Since` returns `ErrLogicalCompactionBeforeSince`. Frontier
regression and int64 overflow are rejected. Compaction validates all folded
sums before mutating state, so an overflow leaves the original state intact.

This is opt-in and is not wired into SQL, storage, or replication defaults.
It is useful when an owner has a safe progress frontier and wants to discard
old differential history without changing the live result. The current
implementation removes old map entries in place; it reduces logical history
but does not promise immediate release of the map's backing buckets. A future
physical rebuild can be evaluated separately because it would add transient
allocation during maintenance.

## Benchmark

Measured on Linux/amd64, AMD Ryzen 9 5950X, with five benchmark samples in one
controlled run. The baseline is the existing `DifferentialMultiset[int]`
`Add` path; the candidate is `LogicalCompaction[int].Add` over the same
4096-update workload. The fold workload compacts 4096 retained records across
128 values through timestamp 4095.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing `DifferentialMultiset.Add` | 8,627 | 6,704 | 12 | 1.00x | 1.00x |
| `LogicalCompaction.Add` | 8,745 | 6,712 | 12 | 1.014x (1.4% slower) | 1.001x (0.12% higher) |
| `LogicalCompaction.CompactThrough` | 330,258 | 9,470 | 11 | maintenance-only | maintenance-only |

Raw five-sample results:

```text
Existing DifferentialMultiset.Add: 8527, 8325, 9937, 9071, 8627 ns/op
LogicalCompaction.Add:             9090, 8582, 8607, 8745, 9736 ns/op
LogicalCompaction.CompactThrough:  338924, 319258, 330258, 331670, 323821 ns/op
```

The add-path overhead is small but not a claim of a speedup. The value of this
feature is bounded-history maintenance, which the baseline structure does not
provide. The benchmark can be repeated with:

```text
make benchmark-m212-logical-compaction
```

