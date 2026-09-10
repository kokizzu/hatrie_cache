# Incremental Recursive Reachability

`hatSql.IncrementalRecursiveReachability` provides both the append-only,
monotone path and an opt-in mutable path for the Materialize-inspired
recursive dataflow idea. It maintains the positive transitive closure of a
directed graph and emits signed `DifferentialRow` updates.

`NewIncrementalRecursiveReachability` remains append-only and stores only
edge identities plus closure state. `NewMutableIncrementalRecursiveReachability`
also retains edge endpoints so `Apply` can maintain arbitrary edge changes.

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

## Mutable Example

```go
reachability := hatSql.NewMutableIncrementalRecursiveReachability()
_, err := reachability.Append([]hatSql.RecursiveReachabilityEdge{
    {Key: "ab", From: "a", To: "b"},
    {Key: "bc", From: "b", To: "c"},
})
updates, err := reachability.Apply([]hatSql.RecursiveReachabilityMutation{
    {Kind: hatSql.RecursiveReachabilityDelete, Key: "bc"},
    {
        Kind: hatSql.RecursiveReachabilityInsert,
        Key:  "bd",
        From: "b",
        To:   "d",
    },
})
```

The delete emits negative `b -> c` and `a -> c` rows. The insert emits
positive `b -> d` and `a -> d` rows. A batch is validated before any graph
state is published.

## Contract

- Edge keys must be non-empty and unique for the lifetime of the maintainer.
- Source and target nodes must be non-empty and cannot contain NUL bytes.
- A complete input batch is validated before any state is published.
- Cycles are supported; each reachable pair is emitted at most once.
- `Reachable` is safe to call concurrently with `Append`.
- The default constructor is append-only and rejects `Apply` with
  `ErrIncrementalRecursiveReachabilityMutationsDisabled`.
- The mutable constructor accepts unique-key `INSERT`, `UPDATE`, and `DELETE`
  mutations. Failed batches leave both edges and closure state unchanged.
- Mutable removals recompute only source nodes that can cross a changed edge;
  isolated terminal-edge deletes and endpoint-only updates use direct delta
  paths. Dense or highly connected changes retain the localized graph-search
  fallback and can approach full rebuild cost.

The implementation stores edge identities, forward reachable pairs, and
reverse ancestor pairs. Memory is proportional to the retained closure.
Mutable mode additionally retains one endpoint pair per edge, so it is
explicit and is not enabled implicitly by the SQL executor.

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

### Mutable Edge Maintenance Benchmark

Command: `make benchmark-m064-mutable-recursive-reachability`.

The workload uses 1,024 independent directed edges. The parent baseline
rebuilds the closure after one edge delete or endpoint update. The mutable
maintainer is seeded outside the timer; only one `Apply` operation is timed.
Five samples were run on Linux/amd64 with an AMD Ryzen 9 5950X. `B/op` is
transient operation allocation and does not include the mutable maintainer's
resident edge map.

| Operation | Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Delete | Parent full closure rebuild | 161,072; 155,988; 148,324; 148,174; 148,622 | 148,622 | 114,752 | 1,028 | 1x |
| Delete | Mutable leaf delete | 13,145; 13,435; 13,012; 13,121; 12,977 | 13,121 | 496 | 7 | 11.33x faster |
| Update | Parent full closure rebuild | 152,882; 162,215; 162,463; 155,311; 159,252 | 159,252 | 114,768 | 1,029 | 1x |
| Update | Mutable leaf endpoint update | 19,296; 20,518; 20,560; 20,051; 20,652 | 20,518 | 1,368 | 18 | 7.76x faster |

Mutable delete uses `231x` fewer transient bytes and `147x` fewer allocations
than its parent rebuild. Mutable update uses `83.9x` fewer transient bytes and
`57.2x` fewer allocations. These direct paths target isolated terminal edges;
the tests also cover cycles and mixed graphs through the general fallback. The
default append-only control measured 2,073 ns/op, 996 B/op, and 14 allocs/op in
the same run; its zero-retention behavior remains the default.

## Verification

```text
make test-m064-recursive-reachability
make test-race-m064-recursive-reachability
make benchmark-m064-recursive-reachability
make test-m064-mutable-recursive-reachability
make benchmark-m064-mutable-recursive-reachability
```
