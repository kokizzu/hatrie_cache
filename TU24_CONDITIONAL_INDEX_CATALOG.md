# T-U24 Conditional Index Catalog

`ConditionalIndexCatalog[T, K]` adds schema-level lifecycle and planner
metadata around the existing conditional functional index. A definition has a
stable name, extractor identity, predicate identity, and immutable functions.
The catalog exposes ready/rebuilding state, generation numbers, cardinality,
and deterministic metadata listing.

## Lifecycle

```go
catalog := hatDataStructure.NewConditionalIndexCatalog[Order, string]()
err := catalog.Create(hatDataStructure.ConditionalIndexDefinition[Order, string]{
	Name:          "open-by-customer",
	ExtractorName: "customer_id",
	PredicateName: "status = 'open'",
	Extractor:     func(order Order) string { return order.CustomerID },
	Predicate:     func(order Order) bool { return order.Status == "open" },
})
```

`Upsert`, `Delete`, and `LookupIDs` use the named index. `Metadata` and
`ListMetadata` provide planner-visible predicate/extractor identities, state,
generation, entry count, and distinct-key count. A planner can include the
generation in a cached plan and invalidate it after a rebuild or drop.

## Rebuild behavior

`Rebuild` constructs a replacement from a caller-supplied row snapshot. Reads
continue against the previous index while the replacement is built. Writes are
rejected with `ErrConditionalIndexCatalogRebuilding`, preventing updates from
being lost during the atomic pointer swap. A successful rebuild increments the
generation; a failed build restores the previous ready state and index.

The catalog does not infer predicate determinism, extract SQL expressions, or
wire itself into a schema/SQL planner. The schema layer supplies stable names,
row snapshots, and the policy for retrying a rebuild.

## Measured cost

Measured on Linux/amd64, AMD Ryzen 9 5950X, with five `-benchmem` samples.
The direct conditional index is the control workload.

| Workload | Direct index | Catalog | Relative result | Memory |
| --- | ---: | ---: | ---: | --- |
| Hot admitted `Upsert` | 27.44 ns/op | 41.93 ns/op | 1.53x slower | 0 -> 0 B/op; 0 -> 0 allocs/op |
| Hot `LookupIDs` | 25.87 ns/op | 39.28 ns/op | 1.52x slower | 8 -> 8 B/op; 1 -> 1 alloc/op |

The extra read/write registry lock buys lifecycle fencing and planner metadata;
it is not a faster replacement for direct use of `ConditionalFunctionalIndex`.

Raw samples:

```text
BenchmarkTU24BeforeConditionalUpsert-32  27.45 26.49 28.25 27.39 27.44 ns/op 0 B/op 0 allocs/op
BenchmarkTU24BeforeConditionalLookup-32  25.87 26.62 26.93 25.37 25.19 ns/op 8 B/op 1 allocs/op
BenchmarkTU24AfterCatalogUpsert-32       39.58 42.39 41.93 43.15 40.49 ns/op 0 B/op 0 allocs/op
BenchmarkTU24AfterCatalogLookup-32       40.59 39.28 37.48 37.29 41.13 ns/op 8 B/op 1 allocs/op
```

Reproduce with `make benchmark-tu24` and `make verify-tu24`.
