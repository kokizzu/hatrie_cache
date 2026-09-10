# Incremental Recursive Reachability

`hatSql.IncrementalRecursiveReachability` is the append-only, monotone portion
of the Materialize-inspired recursive dataflow idea. It maintains the
positive transitive closure of a directed graph and emits only newly
discovered pairs as `DifferentialRow` updates.

## Example

```go
reachability := hatSql.NewIncrementalRecursiveReachability()
updates, err := reachability.Append([]hatSql.RecursiveReachabilityEdge{
    {Key: "ab", From: "a", To: "b"},
    {Key: "bc", From: "b", To: "c"},
})
```

The first append emits `a -> b`, `b -> c`, and the recursively discovered
`a -> c` pair. Each output has a NUL-delimited `from`/`to` key, `Diff: 1`, and
a row containing the `from` and `to` fields. A later `c -> d` edge emits the
new `a -> d`, `b -> d`, and `c -> d` pairs.

## Contract

- Edge keys must be non-empty and unique for the lifetime of the maintainer.
- Source and target nodes must be non-empty and cannot contain NUL bytes.
- A complete input batch is validated before any state is published.
- Cycles are supported; each reachable pair is emitted at most once.
- `Reachable` is safe to call concurrently with `Append`.
- Deletes and updates are intentionally unsupported. Rebuild from the current
  edge set when removals are required.

The implementation stores edge identities, forward reachable pairs, and
reverse ancestor pairs. Memory is proportional to the retained closure, so
this primitive is appropriate for bounded or naturally monotone graphs and is
not enabled implicitly by the SQL executor.

## Benchmark

Command: `make benchmark-m064-recursive-reachability`.

The workload starts with 1,024 independent edges and attaches one new leaf to
one existing root. Five samples were run on Linux/amd64 with an AMD Ryzen 9
5950X. The full-recompute baseline is a map-backed breadth-first closure
rebuild in the benchmark; it is an algorithmic comparison, not a claim that
the current general SQL executor has this exact graph query shape.

| Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Full closure before implementation | 157,098; 153,578; 142,785; 143,547; 142,099 | 143,547 | 114,832 | 1,031 |
| Full closure after implementation | 145,970; 149,561; 143,744; 147,748; 154,600 | 147,748 | 114,832 | 1,031 |
| Incremental leaf append | 1,534; 1,552; 1,446; 1,478; 1,421 | 1,478 | 990 | 14 |

Against the post-change full rebuild, the incremental append is about `100x`
faster, uses `116x` fewer transient bytes, and makes `74x` fewer allocations.
The full rebuild path itself is unchanged; its small median difference is
normal benchmark variance.

## Verification

```text
make test-m064-recursive-reachability
make test-race-m064-recursive-reachability
make benchmark-m064-recursive-reachability
```
