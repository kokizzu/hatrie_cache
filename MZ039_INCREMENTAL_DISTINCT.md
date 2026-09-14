# Materialize-Inspired Incremental Distinct

`hatSql.IncrementalDistinct` is an importable stateful differential operator
for maintaining set membership across batches. It is useful when a source
contains duplicate weighted rows and consumers need an exact distinct
relation without rebuilding the complete source after every update.

## Contract

```go
distinct := hatSql.NewIncrementalDistinct()

changes, err := distinct.Apply([]hatSql.DifferentialRow{
	{Key: "a", Diff: 2, Row: hatSql.Row{"value": "A"}},
	{Key: "b", Diff: 1, Row: hatSql.Row{"value": "B"}},
})
// changes contains +a and +b, once each.

changes, err = distinct.Apply([]hatSql.DifferentialRow{
	{Key: "a", Diff: -1},
	{Key: "a", Diff: -1},
})
// changes contains -a only when the second retraction reaches zero.
```

`DifferentialRow.Key` is the stable logical identity. Positive `Diff` adds to
the key's multiplicity, and negative `Diff` retracts it. The output is a set:
it emits exactly one `Diff: 1` transition when a key enters and one `Diff: -1`
transition when it leaves. Intermediate multiplicity changes emit nothing.
`Snapshot` returns one positive row per active key, sorted by key; `AllRows` is
an alias for relation-style callers.

The first positive update owns a cloned row payload. Later positive updates may
omit the payload, or provide the same row; a different payload for an active
key is rejected. Retractions may omit the payload and use the retained row for
the emitted transition. Snapshot rows and transition rows are independently
cloned, so callers cannot mutate operator state through returned maps or
top-level byte slices.

## Atomicity And Limits

`Apply` validates a complete multi-row batch before publishing any state. Key
validation, negative multiplicity, overflow, and payload conflicts leave both
state and output unchanged. A batch may contain multiple transitions for one
key; transitions are emitted in input order, while snapshots are deterministic
by key.

The public differential format uses signed `int64` values, so a key's stored
multiplicity is bounded at `math.MaxInt64`. This avoids accepting internal
state that cannot be represented by the public input/output contract.

The operator is single-writer and does not add locks. Callers sharing one
instance across goroutines must synchronize access.

## Measurement

Command: `make benchmark-mz039-incremental-distinct`

Workload: 10,000 active keys, repeated valid add/retract updates, and one
membership transition per operation. The rebuild baseline scans all 10,000
counts and materializes an equivalent transition row on every update. The
incremental path seeds the same relation outside the timer and updates one
key through its persistent multiplicity map. CPU: AMD Ryzen 9 5950X 16-Core
Processor, Linux amd64.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Full distinct scan and transition materialization | 3,784 | 343 | 2 | baseline |
| Incremental multiplicity update and transition materialization | 633.1 | 722 | 5 | 5.98x faster |

The incremental path uses about 2.10x more transient bytes because it owns
the active row for future retractions and returns an independent transition
payload. It avoids the `O(N)` scan and is intended for update-heavy stateful
distinct views; it is not an implicit SQL plan rule.

Raw repeated samples are recorded in [BENCHMARK.md](BENCHMARK.md#mz-039-incremental-distinct).
