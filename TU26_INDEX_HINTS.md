# T-U26 Index Hints And Strategy Inspection

## Purpose

`hatDataStructure.IndexSelectionCatalog` provides an importable, opt-in
selection layer for callers that already have multiple indexes for a query.
It lets a caller request an index by name or strategy and inspect why a
candidate won. Existing index lookup behavior is unchanged; this catalog does
not run automatically and does not change default query planning.

## Usage

```go
catalog, err := hatDataStructure.NewIndexSelectionCatalog([]hatDataStructure.IndexCandidate{
	{
		Name:          "account",
		Strategy:      hatDataStructure.IndexStrategyHash,
		EstimatedCost: 20,
		Capabilities:  hatDataStructure.IndexCapabilityEquality,
	},
	{
		Name:          "created",
		Strategy:      hatDataStructure.IndexStrategyOrdered,
		EstimatedCost: 30,
		Capabilities:  hatDataStructure.IndexCapabilityRange,
	},
})
if err != nil {
	return err
}

decision, err := catalog.Select(
	hatDataStructure.IndexOperationEquality,
	hatDataStructure.IndexHint{},
)
if err != nil {
	return err
}
fmt.Println(decision.Candidate.Name, decision.Reason)
```

The catalog validates and copies metadata once. `Select` is read-only after
construction and performs no per-call allocation. Candidates are ordered by
the caller-provided `EstimatedCost`; equal costs use candidate name and then
strategy as deterministic tie-breakers.

Use a required name or strategy when the caller must enforce a policy:

```go
decision, err := catalog.Select(
	hatDataStructure.IndexOperationRange,
	hatDataStructure.IndexHint{
		Strategy: hatDataStructure.IndexStrategyOrdered,
		Required: true,
	},
)
```

An optional hint falls back to the normal lowest-cost compatible candidate when
the hint cannot be honored. Required hints return an error instead. `Explain`
returns one evaluation per candidate and is intended for diagnostics, query
plan inspection, or an admin endpoint; it allocates the evaluation slice.

## Supported Metadata

Strategies are hash, ordered, functional, multikey, bitset, and full-text.
Operations are equality, range, prefix, and contains. A candidate must declare
at least one matching capability. Candidate names are trimmed, must be valid
UTF-8, and are unique within a catalog.

## Measurement

Measured with:

```text
make codex-tu26-bench
```

On the test host, five runs produced these representative medians:

| Path | ns/op | B/op | allocs/op | Relative to retained baseline |
| --- | ---: | ---: | ---: | ---: |
| Existing four-entry linear baseline | 4.223 | 0 | 0 | 1.00x |
| `IndexSelectionCatalog.Select` | 77.80 | 0 | 0 | 18.42x |
| `IndexSelectionCatalog.Explain` | 180.3 | 192 | 1 | 42.69x |

The baseline is a minimal four-entry linear benchmark, not an equivalent
planner. The new API trades a small control-plane CPU cost for validation,
capability filtering, deterministic hints, and explainability. Since it is
opt-in and does not sit on the existing data lookup path, the benchmark does
not justify changing default lookup behavior. `Explain` should stay out of the
normal read path.

## Safety

If hints are exposed through SQL, HTTP, or another user-controlled interface,
the caller must authorize index names and strategies. A hint is a planning
policy, not an authorization boundary. Required unsupported hints fail closed;
optional hints do not weaken capability checks when they fall back.
