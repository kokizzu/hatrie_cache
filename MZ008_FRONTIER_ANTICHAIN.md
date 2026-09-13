# MZ-008 Compact Frontier Antichain

Materialize-style multi-dimensional frontiers are represented by the public
`hatPipeline.FrontierAntichain` type. It is a standalone building block for
partition or region frontiers; the existing scalar `FrontierRegistry` remains
unchanged.

## Semantics

Points use component-wise ordering. Point `a` covers point `b` when every
component of `a` is less than or equal to the corresponding component of `b`.
The antichain retains only minimal, mutually incomparable points:

- inserting a point already covered by a retained point is a no-op;
- inserting a new point removes retained points that it covers;
- an incomparable insertion is rejected atomically when `MaxPoints` is full;
- `Covers` checks readiness without allocating; and
- `Snapshot` returns a detached flat `[]uint64` payload.

The type is deliberately not internally synchronized. Protect it with the
caller’s partition/dataflow lock when shared between goroutines.

## API And Example

```go
antichain, _ := hatPipeline.NewFrontierAntichain(
	2,
	hatPipeline.FrontierAntichainOptions{},
)
antichain.Insert([]uint64{5, 1})
antichain.Insert([]uint64{1, 5})

covered, _ := antichain.Covers([]uint64{5, 5})
fmt.Println(covered, antichain.Len())
```

Output:

```text
true 2
```

`MaxPoints` defaults to `1024`. `InitialPoints` defaults to zero, preserving
allocation-free construction and lazy growth. Set `InitialPoints` when the
expected antichain size is known; it avoids cumulative growth copies and is
bounded by `MaxPoints`.

## Benchmark

Five `-benchmem` samples ran on Linux `amd64` with an AMD Ryzen 9 5950X. The
workload inserts 256 incomparable four-dimensional points.

| Operation | Median CPU | Allocated memory | Allocations |
| --- | ---: | ---: | ---: |
| Flat antichain, lazy growth | 135,320 ns/op | 25,184 B/op | 10 allocs/op |
| Flat antichain, presized | 127,779 ns/op | 8,192 B/op | 1 alloc/op |
| Nested-slice reference | 132,264 ns/op | 14,720 B/op | 257 allocs/op |
| `Covers` readiness check | 3.445 ns/op | 0 B/op | 0 allocs/op |
| Flat `Snapshot` | 1,493 ns/op | 8,192 B/op | 1 alloc/op |

Presizing is about `1.04x` faster than the nested reference, uses `44%` less
allocated memory, and performs `256x` fewer allocations. Lazy growth retains
the same low allocation count but has higher cumulative allocation due to
geometric backing-slice growth; that is why the reservation is explicit rather
than the default.

Reproduce with:

```text
make benchmark-mz008
```

