# M-U45 Incremental Join Index Selection

`hatSql.TypedTableJoinArrangements.AcquireBest` is an opt-in selector for
callers whose join predicate can change between executions. The caller gives
the registry a bounded list of compatible exact equi-join definitions and an
estimated retained pair count for each candidate. The registry selects the
smallest estimate, prefers an already-maintained arrangement on a tie, and
returns a reference-counted lease for the selected arrangement.

## Example

```go
registry, err := hatSql.NewTypedTableJoinArrangements(left, right)
if err != nil {
	return err
}

lease, selection, err := registry.AcquireBest([]hatSql.TypedTableJoinArrangementCandidate{
	{
		Definition:         hatSql.TypedTableJoinDefinition{LeftField: "tenant_id", RightField: "tenant_id"},
		EstimatedStateRows: 100000,
	},
	{
		Definition:         hatSql.TypedTableJoinDefinition{LeftField: "region", RightField: "region"},
		EstimatedStateRows: 4000,
	},
})
if err != nil {
	return err
}
defer lease.Release()

// selection.Definition is the maintained key. Apply ordered table changes to
// lease as usual; the selected arrangement stays incrementally correct.
_ = selection
```

## Contract

- The candidate list is capped at 256 entries.
- Candidates whose fields are missing or whose physical kinds differ are
  ignored. An all-incompatible list returns an error without creating state.
- Estimates affect selection only; they never change join results.
- Equal estimates prefer an existing arrangement, then deterministic field
  ordering. Duplicate definitions do not create duplicate maintained state.
- The selected arrangement is the only arrangement created or retained by
  `AcquireBest`; callers still release leases when the predicate is no longer
  needed.
- `Acquire` remains available for callers with a fixed, already-known
  definition and is unchanged.

The method does not guess cardinalities, create indexes in the SQL parser, or
silently switch an active lease. Query/planner code owns candidate generation
and should refresh estimates when predicates change. This keeps correctness
and operational memory ownership explicit while providing one safe registry
boundary for adaptive selection.

## Measurement

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, reusing one live
arrangement and comparing the existing exact `Acquire` path with a two-choice
`AcquireBest` request:

| Workload | Clean c840 median ns/op | Feature median ns/op | Feature B/op | Feature allocs/op | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Existing exact `Acquire` | 90.3 | 95.0 | 53 | 2 | 1.05x; no allocation change |
| `AcquireBest` with two candidates | n/a | 175.7 | 53 | 2 | 1.85x versus feature exact acquire |

The selector adds no allocation or retained arrangement memory. Its cost is
paid only when a caller requests adaptive selection; row updates, join result
maintenance, and fixed-definition `Acquire` do not use this path.

Raw samples:

```text
clean_c840_exact_acquire: 90.26, 89.85, 88.53, 95.00, 92.01 ns/op, 53 B/op, 2 allocs/op
feature_exact_acquire: 92.80, 97.94, 96.94, 95.02, 94.74 ns/op, 53 B/op, 2 allocs/op
acquire_best: 175.7, 174.6, 183.0, 179.8, 173.5 ns/op, 53 B/op, 2 allocs/op
```

Focused verification:

```sh
make test-mu45
make verify-mu45
make benchmark-mu45
```
