# M213: Differential Batch Consolidation

Materialize-style differential dataflow consolidates equal updates before an
update crosses an operator boundary. M213 adds the same explicit boundary to
`hatSql.QuerySubscriptionDeltaBatch`: downstream consumers can call
`Consolidate()` before applying a batch, and `DebeziumChangefeed.Apply` does so
automatically.

## Semantics

- Equal complete row images are combined with signed `int64` multiplicity.
- Zero-sum rows are removed.
- Overflow returns `ErrQuerySubscriptionDeltaOverflow` without returning a
  partial batch.
- Consolidation compares the complete row image, not a downstream primary key.
  Distinct row images that share a key remain distinct so adapters such as
  Debezium can apply their own key-conflict policy.
- Consolidated rows are cloned before they are returned, preserving caller
  ownership and preventing downstream mutation from changing the input batch.
- Batches created by the query subscription publisher are already grouped and
  carry an internal normalized marker. `Consolidate()` therefore returns them
  without another map, row-key, or allocation pass.

Example:

```go
batch := hatSql.QuerySubscriptionDeltaBatch{
    Deltas: []hatSql.QuerySubscriptionDelta{
        {Row: hatSql.Row{"id": int64(7)}, Diff: 1},
        {Row: hatSql.Row{"id": int64(7)}, Diff: -1},
        {Row: hatSql.Row{"id": int64(8)}, Diff: 2},
    },
}
normalized, err := batch.Consolidate()
if err != nil {
    return err
}
// normalized.Deltas contains only id 8 with Diff 2.
```

The marker is private, so it does not add a JSON field or change the existing
wire shape. A batch assembled by another package has no marker and is safely
normalized at the downstream boundary.

## Measurement

Results below use five samples with `-benchmem` and `-benchtime=100ms` on
Linux/amd64 with an AMD Ryzen 9 5950X. The generated paths build the same
128-row differential batch; the boundary path then calls `Consolidate()`.
The unmarked path measures normalization of a prebuilt external batch and is
therefore not an end-to-end comparison with the generated path.

| Workload | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Generated batch before boundary | 137,243 | 71,149 | 970 | 1.00x |
| Generated batch through boundary | 135,877 | 71,151 | 970 | 0.99x, within noise |
| Prebuilt unmarked batch consolidation | 211,710 | 50,361 | 2,128 | n/a |

The normal publisher path shows no allocation increase and no measurable CPU
regression. The unmarked cost is paid only by callers that construct their own
unconsolidated batch; it centralizes correctness at the consumer boundary.

Raw samples:

```text
Generated baseline ns/op: 137243 139704 137935 135212 133024
Generated baseline B/op:  71157  71145  71146  71156  71149
Generated baseline allocs: 970 970 970 970 970

Boundary ns/op: 139548 122490 140473 130318 135877
Boundary B/op:  71154  71151  71150  71149  71151
Boundary allocs: 970 970 970 970 970

Unmarked ns/op: 227437 208520 230309 211710 209783
Unmarked B/op:  50361  50364  50359  50361  50360
Unmarked allocs: 2128 2128 2128 2128 2128
```

Focused verification:

```text
make m213-format
make m213-test
make m213-race
make m213-vet
make m213-benchmark
```
