# T-U26 Index Strategy Hints

This is an opt-in Tarantool-style index strategy catalog for callers that
need to choose or inspect an embedded index. It does not change any existing
index lookup, mutation, or planner default.

## API

Register bounded metadata once during schema or planner setup:

```go
catalog := hatDataStructure.NewDefaultIndexStrategyCatalog()
_ = catalog.Register(hatDataStructure.IndexStrategyDescriptor{
    Name: "orders_by_created_at",
    Kind: hatDataStructure.IndexStrategyOrdered,
    Fields: []string{"created_at"},
    Capabilities: hatDataStructure.IndexStrategyCapabilities{
        Range: true,
        Ordered: true,
    },
    EstimatedCardinality: 1_000_000,
    EstimatedBytes: 24 << 20,
})
```

Use a named hint when the caller already knows the intended strategy:

```go
var resolved hatDataStructure.IndexStrategyDescriptor
err := catalog.ResolveInto(&resolved, hatDataStructure.IndexStrategyHint{
    Name: "orders_by_created_at",
    Field: "created_at",
    Operation: hatDataStructure.IndexOperationRange,
})
```

Use `Suggest` for a one-shot planner lookup, or retain a destination slice and
call `SuggestInto` repeatedly to avoid planner-loop allocations. Suggestions
are filtered by field, operation, and uniqueness, then ranked by estimated
bytes, estimated cardinality, and name. Estimates are advisory only; the
caller still owns actual index compatibility and execution.

The catalog has bounded capacity and bounded name/field lengths. Descriptor
and snapshot results own their field slices, so callers cannot mutate catalog
state. `ResolveInto` and `SuggestInto` are concurrency-safe and allocation-free
after caller-owned scratch storage is warm.

## Tradeoff

The catalog is deliberately not attached to ordinary index operations. It
adds no default read/write overhead, but a caller pays for metadata setup and
for one-shot result copies. Use the reusable `Into` methods in repeated plan
selection paths.

See [BENCHMARK.md](BENCHMARK.md#t-u26-index-strategy-hints) for raw samples.
