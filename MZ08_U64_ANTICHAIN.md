# MZ-08 Compact U64 Antichain

This adds a reusable Materialize-style antichain for multi-dimensional
frontiers. `U64Antichain` keeps only minimal coordinate-wise timestamps: a new
point already covered by an existing point is ignored, while points that make
older points redundant remove those points.

Unlike a conventional `[][]uint64` representation, all points use one flat
row-major `[]uint64`. That removes one slice header and one allocation per
point while keeping the vector width fixed and explicit. The type is safe for
concurrent readers and writers.

```go
antichain, err := hatDataStructure.NewU64AntichainWithOptions(
	hatDataStructure.U64AntichainOptions{
		Dimensions:    2,
		MaxEntries:    1024,
		InitialEntries: 16,
	},
)
if err != nil {
	return err
}

_, _ = antichain.Add([]uint64{10, 20})
_, _ = antichain.Add([]uint64{12, 18}) // incomparable with [10, 20]
covered, _ := antichain.Covers([]uint64{11, 21})
// covered is true because [10, 20] <= [11, 21].

points, _ := antichain.Snapshot(nil)
// points is flattened: [10, 20, 12, 18].
```

`Add` is atomic with respect to `MaxEntries`: it evaluates dominance and the
resulting size before changing the stored frontier. `Reset` clears points but
retains the backing allocation and configuration. `Covers` performs a
zero-allocation read, and `Snapshot(dst)` reuses a destination with enough
capacity.

## Benchmark

Command:

```text
make benchmark-u64-antichain
```

Environment: Linux amd64, AMD Ryzen 9 5950X, Go benchmark `-benchtime=1x
-count=5`. The workload inserts 512 pairwise-incomparable two-dimensional
points. Both benchmarks retain the completed structure; the compact version
uses `InitialEntries: 512` so the comparison measures representation rather
than repeated slice growth.

| Representation | Median build time | Median allocated bytes | Median allocations | Result |
|---|---:|---:|---:|---:|
| Flattened `U64Antichain` | 266,777 ns/op | 8,256 B/op | 2 allocs/op | baseline |
| Conventional `[][]uint64` | 481,443 ns/op | 21,760 B/op | 513 allocs/op | 1.81x slower, 2.64x more bytes, 256x more allocs |

The compact representation is most useful when frontier width and an upper
bound are known, such as source partitions or worker dimensions. With
`InitialEntries == 0`, growth remains lazy and avoids reserving unused memory,
but an expanding frontier can incur additional transient backing-array
allocations. `MaxEntries` should be set when untrusted or unbounded input must
be governed.

## Scope

This is an opt-in library primitive in `hatDataStructure`. Existing scalar
frontier registries and SQL behavior are unchanged. A caller that uses it for
timestamps must define the coordinate order and retain the same dimensions
across producers, snapshots, and restores. The package does not infer a
partial order or automatically replace existing frontier implementations.

Tests cover dominance and incomparability, duplicate suppression, capacity
rejection without mutation, dimension validation, snapshot reuse, reset/reuse,
nil receivers, public imports, and concurrent access. Run
`make test-u64-antichain`, `make race-u64-antichain`,
`make test-u64-antichain-package`, and `make vet-u64-antichain` for the focused
verification set.
