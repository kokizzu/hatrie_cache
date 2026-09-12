# T-G11 Conditional Functional Index

This adds a reusable conditional secondary-index primitive inspired by
Tarantool tuple-space indexes and SQL partial indexes. A
`ConditionalFunctionalIndex[T, K]` evaluates an admission predicate before the
key extractor. Rejected values are never inserted, and replacing an admitted
value with a rejected one removes the old posting.

The implementation wraps the existing `FunctionalIndex[T, K]`, so it retains
the same concurrent access, stable-ID, posting-order, `LookupInto`, and
`LookupIDsInto` behavior. Comparable structs and arrays can be used as `K` for
multi-field tuple keys.

```go
type Order struct {
	Region string
	Status string
	Amount int64
}

index, err := hatDataStructure.NewConditionalFunctionalIndex(
	func(order Order) struct {
		Region string
		Status string
	} {
		return struct {
			Region string
			Status string
		}{order.Region, order.Status}
	},
	func(order Order) bool { return order.Status == "open" },
	1024,
)
if err != nil {
	return err
}

_ = index.Upsert(42, Order{Region: "ap-southeast", Status: "open", Amount: 900})
ids := index.LookupIDs(struct {
	Region string
	Status string
}{"ap-southeast", "open"})
// ids is []uint64{42}.
```

`Upsert` returns nil for a rejected value because rejection is a normal index
admission result. `Delete`, `Len`, `DistinctKeys`, `Contains`, `Clear`, and
the value/ID lookup methods are available for maintenance and reads.

## Benchmark

Command:

```text
make benchmark-conditional-index
```

Environment: Linux amd64, AMD Ryzen 9 5950X, Go benchmark `-count=5`. Reported
values are medians of the five samples.

| Workload | Conditional index | Full functional index | Conditional result |
|---|---:|---:|---:|
| Repeated upsert, 90% rejected | 51.87 ns/op, 0 B/op, 0 allocs/op | 67.81 ns/op, 0 B/op, 0 allocs/op | 1.31x faster |
| Build 10,000 rows, 10% admitted | 363,400 ns/op, 254,896 B/op, 25 allocs/op | 872,123 ns/op, 2,194,064 B/op, 88 allocs/op | 2.40x faster, 8.61x lower bytes, 3.52x fewer allocs |

The full-index baseline stores all 10,000 rows; the conditional index stores
only the 1,000 admitted rows. This is the intended trade: sparse or hot-subset
indexes use materially less memory and update work. A predicate that admits
nearly every row will provide little benefit, and predicate execution itself
is still caller code. The benchmark does not claim that the conditional index
replaces a full index for queries that need rejected rows.

## Scope

This is a library primitive in `hatDataStructure`. It does not silently alter
existing SQL index behavior and does not automatically infer predicates from
SQL statements. A caller or future SQL planner integration must keep the
predicate deterministic, use the same predicate when maintaining and querying
the index, and rebuild the index when predicate or key semantics change.

Focused tests cover admission, replacement in both directions, deletion,
clear/reuse, nil inputs, zero-allocation point membership checks, public API
use, and concurrent readers/writers. Run `make test-conditional-index`,
`make race-conditional-index`, `make test-conditional-index-package`, and
`make vet-conditional-index` for verification.
