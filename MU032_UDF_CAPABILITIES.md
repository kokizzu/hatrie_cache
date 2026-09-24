# M-U32 UDF Capability Classification

Custom SQL functions can now carry explicit metadata for callers that build
incremental or cache-aware plans. `hatSql.FunctionDefinition` adds:

- `Deterministic`: equal inputs are declared to produce equal outputs.
- `Pure`: evaluation has no observable side effects and returns a value safe
  for SQL expression reuse. Purity requires `Deterministic: true`.
- `Monotonicity`: `constant`, `non_decreasing`, or `non_increasing`; the
  zero value is unknown.
- `Retractable`: the caller declares that the function has a valid inverse or
  update contract for its differential use.

The metadata is a declaration, not a proof generated from a UDF body. The
registry never infers it from `GO`, `LUA`, `WASM`, or `JS`, and it never uses it
to bypass normal function evaluation.

## Registration

```go
registry := hatCache.NewSQLFunctionRegistry()
err := registry.Register(hatCache.SQLFunctionDefinition{
    Name:          "score_boost",
    Arguments:     []string{"score"},
    ArgumentTypes: []string{"INTEGER"},
    Language:      "GO",
    Source:        "return score + 1",
    Deterministic: true,
    Pure:          true,
    Monotonicity:  hatCache.SQLFunctionMonotonicityNonDecreasing,
    Retractable:   true,
})
```

`hatSql.NormalizeFunctionCapabilities` accepts case-insensitive values and
normalizes hyphens to underscores. A purity, monotonicity, or retraction
declaration without `Deterministic: true` is rejected. Unknown values are
rejected rather than silently downgraded. Existing definitions with none of
these fields keep the conservative unknown state.

## Catalog Lookup

`SQLFunctionRegistry.Definition(name)` returns one definition and
`Definitions()` returns all definitions in name order. Both return defensive
copies, including argument slices, so a planner cannot mutate the live
registry accidentally. The same methods are available on the lower-level
`hatSql.Registry`. The optional `FunctionCapabilityResolver` uses a separate
allocation-free boolean lookup for repeated expression planning.

The fields use `omitempty` JSON tags. Old persisted function files continue to
load with unknown capabilities; new files retain the declarations through the
existing atomic persistence path.

## Safe Use

- Treat metadata from an untrusted persistence file as untrusted input. Only
  grant stronger classifications after validating the function source and its
  operational policy.
- `Deterministic` does not imply monotonicity or retraction.
- `Pure` does not imply deterministic behavior; both declarations are
  required for expression reuse.
- `Monotonicity` is an advisory function-level property. A planner that needs
  per-argument proofs must add that stricter contract before using it.
- `Retractable` does not make a scalar function or aggregate transactionally
  reversible by itself; the caller owns the differential update semantics.
- The SQL evaluator uses an opt-in fast path when a resolver implements
  `hatSql.FunctionCapabilityResolver`: pure deterministic UDF calls whose
  arguments are all literals execute once per batch. Metadata-free, impure,
  parameterized, and row-dependent calls retain the existing path.

Composite results are copied for each output row for the supported byte-slice,
array, and string-map shapes. The fast path does not change runtime sandbox
limits or authorization.

This is the catalog boundary needed for Materialize-style safe incremental
arrangements and ClickHouse-style deterministic expression reuse. The next
integration step can consume the metadata only where the planner has enough
schema, argument, and authorization context to validate the claim.

## Measurement

Commands:

```text
make benchmark-mu032-function-capabilities-baseline
make benchmark-mu032-function-capabilities
```

Both commands run five one-second samples on Linux/amd64 with an AMD Ryzen 9
5950X. The execution comparison evaluates the same 256-row bounded `GO` UDF;
the only difference is whether capability fields are present. The lookup case
measures one defensive catalog copy.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing unclassified UDF execution | 16,191 | 4,888 | 3 | baseline |
| Classified UDF execution | 15,806 | 4,888 | 3 | 0.98x, no measurable regression |
| Defensive `Definition` lookup | 179.1 | 48 | 3 | lookup-only |

The 2.4% execution difference is within normal benchmark variance and is not
treated as a performance win. Capability fields add no measured work or
allocation to the existing non-folded evaluation path. The lookup cost is the
intentional price of returning independent slices; callers should cache
definitions when planning repeated executions. The planner fast path uses the
separate boolean capability lookup and does not pay that defensive-copy cost.

The literal-batch fast path is measured separately with
`make benchmark-m045-udf-fastpath`; its raw samples and fallback comparison
are recorded in [BENCHMARK.md](BENCHMARK.md#mu-045-pure-literal-udf-fast-path).

Raw samples are recorded in
[BENCHMARK.md](BENCHMARK.md#mu-032-udf-capability-classification).

## Verification

```text
make test-mu032-function-capabilities
make test-mu032-package
make race-mu032-function-capabilities
make vet-mu032-function-capabilities
make verify-mu032-function-capabilities
```
