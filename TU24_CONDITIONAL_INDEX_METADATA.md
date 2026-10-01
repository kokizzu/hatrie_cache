# T-U24 Conditional Index Metadata

`hatSchema.IndexDefinition` now supports an opt-in conditional-index contract
through `Predicate` and `PredicateColumns`. `SpaceCatalog.ConditionalIndexes`
returns the clone-safe declarations for one named space so a planner can see
which index predicate must be proven before using that index.

```go
catalog, err := hatSchema.NewSpaceCatalog([]hatSchema.SpaceDefinition{
	{
		Name: "events",
		Source: hatSchema.Source{
			Name: "events",
			Columns: []hatSchema.Column{
				{Name: "tenant", Type: hatSchema.TypeText},
				{Name: "state", Type: hatSchema.TypeText},
			},
		},
		Indexes: []hatSchema.IndexDefinition{{
			Name:             "tenant_open",
			Kind:             hatSchema.IndexKindTree,
			Columns:          []string{"tenant"},
			Predicate:        "state = 'open'",
			PredicateColumns: []string{"state"},
		}},
	},
})
if err != nil {
	panic(err)
}

indexes, exists := catalog.ConditionalIndexes("events")
// exists == true; indexes[0] describes tenant_open and its state dependency.
```

## Contract

- A non-empty `Predicate` requires one or more `PredicateColumns`.
- `PredicateColumns` without a predicate is rejected.
- Predicate dependencies must name existing source columns and may not repeat.
- Predicate text is normalized for surrounding whitespace but is not parsed or
  executed. The caller must provide deterministic, side-effect-free semantics
  and must maintain the runtime index.
- `ConditionalIndexes` distinguishes an unknown space (`false`) from a known
  space with no conditional indexes (`true`, empty slice).
- Returned index definitions and dependency slices are independent copies.
- Existing nonconditional index declarations and default SQL behavior are
  unchanged.

The metadata is intentionally separate from execution. This prevents a schema
declaration from silently creating an index whose predicate cannot be proven,
while still giving an SQL planner a bounded dependency list for eligibility,
explain output, and invalidation decisions.

## Benchmark

The benchmark uses one space with 32 tree indexes, four conditional indexes,
and a source schema with 35 columns. Five `-benchmem` samples ran on an AMD
Ryzen 9 5950X.

| Planner metadata path | Median | Bytes/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Manual `Lookup` then filter | 3,001 ns | 7,360 | 38 | baseline |
| `ConditionalIndexes` | 1,105 ns | 4,224 | 9 | 2.72x faster; 42.6% fewer bytes; 4.22x fewer allocs |

See the raw samples in [BENCHMARK.md](BENCHMARK.md#t-u24-conditional-index-metadata).
