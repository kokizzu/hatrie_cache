# MZ-026 Adaptive Dictionary Arrangements

## Behavior

`TypedTableSortedArrangementDefinition.DictionaryAdaptive` enables automatic
dictionary selection for a string sort field. The same option is available on
each `TypedTableSortedArrangementOrder` in a composite `OrderBy` definition.
It is disabled by default, so existing arrangements retain their current raw
string behavior.

Admission is bounded and deterministic by policy:

- the snapshot must contain at least one valid string value;
- at most 32 distinct values may be observed;
- distinct values must be no more than one eighth of the snapshot row count;
- the probe uses a fixed-capacity slice, so admission does not allocate a
  temporary hash map.

`DictionaryEncoded` takes precedence when both flags are set. Explicit
dictionary encoding keeps its existing behavior and never demotes. Adaptive
encoding demotes to raw strings when a later insert or update takes the live
distinct count above 32. The demotion copies the live values before releasing
the dictionary, then continues processing the change, so callers see the same
rows and ordering.

Only string order fields can use either dictionary option. For a composite
arrangement, set `DictionaryAdaptive` independently on each string order
field:

```go
arrangement, err := NewTypedTableSortedArrangement(table,
	TypedTableSortedArrangementDefinition{
		OrderBy: []TypedTableSortedArrangementOrder{
			{Field: "region", DictionaryAdaptive: true},
			{Field: "created_at", Descending: true},
		},
	})
```

## Measurements

Command: `make benchmark-mz026-adaptive`.

Five samples per case, Linux amd64, AMD Ryzen 9 5950X. Each case builds a
4,096-row arrangement; the raw and adaptive cases in each cardinality group
use the same input distribution. Values are medians from the final run.
`retained_sort_string_bytes` is the string payload retained by the sort
arrangement, not the complete heap footprint.

| Case | Distinct strings | ns/op | B/op | allocs/op | Retained string bytes | Relative CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw default | 16 | 5,698,866 | 2,163,458 | 8,267 | 40,960 | 1.00x |
| Adaptive, admitted | 16 | 5,673,378 | 2,165,793 | 8,284 | 160 | 1.00x, within noise |
| Raw default | 4,096 | 5,438,584 | 2,163,456 | 8,267 | 40,960 | 1.00x |
| Adaptive, rejected | 4,096 | 5,335,163 | 2,163,452 | 8,267 | 40,960 | 1.02x, within noise |

The admitted case reduces retained sort-string payload by 256x (99.61%) while
adding about 2.3 KB/op and 17 allocations for arrangement construction. The
fixed-slice probe removed the temporary map used by the first implementation;
the final low-cardinality case is about 1.8 KB/op and 3 allocations lower than
that probe. High-cardinality input remains raw and does not pay dictionary
storage costs. The feature is opt-in because admission still performs a
bounded cardinality scan and dictionary bookkeeping during construction.

## Verification

- `make test-mz026-adaptive`
- `make race-mz026-adaptive`
- `make vet-mz026-adaptive`
- `make benchmark-mz026-adaptive`

The whole-package target `make test-mz026-package` still reports the three
pre-existing typed-table checkpoint failures at the parent revision:
`TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate`,
`TestTypedTableAggregateArrangementCheckpointIsDeterministic`, and
`TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`.
