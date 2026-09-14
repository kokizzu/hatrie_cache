# Materialize-Inspired Incremental Top-K

`hatSql.IncrementalTopK` is an importable exact differential operator for a
bounded ordered result. It is useful when a relation receives frequent signed
updates but consumers repeatedly need only the best `K` rows.

## Contract

```go
topK, err := hatSql.NewIncrementalTopK(hatSql.IncrementalTopKDefinition{
	K:          2,
	Descending: true,
	OrderKey: func(row hatSql.Row) (interface{}, error) {
		return row["score"], nil
	},
})

changes, err := topK.Apply([]hatSql.DifferentialRow{
	{Key: "a", Diff: 1, Row: hatSql.Row{"score": int64(10)}},
	{Key: "b", Diff: 1, Row: hatSql.Row{"score": int64(8)}},
	{Key: "c", Diff: 1, Row: hatSql.Row{"score": int64(7)}},
})
// changes contains +a and +b; c is retained in the relation but outside K.

snapshot := topK.Snapshot()
// snapshot is [{Key: "a", Diff: 1}, {Key: "b", Diff: 1}].
```

`DifferentialRow.Key` is the stable logical row identity. A positive `Diff`
adds multiplicity and a negative `Diff` retracts it. A row can be incremented
without a payload; a supplied payload for an existing key must match the
original row. Replacing a row is represented by a retraction followed by an
insertion, commonly in one atomic `Apply` batch.

The order callback uses the package SQL ordering exposed by `hatSql.Compare`.
`Descending` reverses non-equal order values, and equal values use the key as
an ascending deterministic tie-break. `nil` follows the same NULL ordering as
the SQL comparator. The public differential format is signed `int64`, so
multiplicity above `math.MaxInt64` is rejected without changing state.

`Apply` validates all updates before publishing any state. Invalid keys,
negative multiplicity, overflow, callback failures, and conflicting row
payloads are atomic failures. The result contains only changes to the bounded
Top-K view, with removals first and additions second. `Snapshot` returns the
current bounded view; `AllRows` is available when a caller needs the complete
active relation for diagnostics or validation.

The operator owns cloned row maps and top-level byte slices. It is single
writer and does not add locks; callers sharing one instance across goroutines
must synchronize access.

## Data Structure

The implementation uses a randomized treap ordered by `(order value, key)`.
Each logical key occupies one node and stores its multiplicity, so duplicate
weights do not duplicate row storage. Insert, retract, and reinsert operations
are expected `O(log N)`; reading the bounded result is `O(log N + K)` for the
visited tree prefix. No subtree aggregate is retained because the bounded
collector stops after `K`, keeping per-node state small.

This is an operator primitive, not an automatic SQL optimization. SQL plan
selection, distributed worker exchange, and persistence of operator state
remain caller-owned until a future integration contract can preserve their
semantics.

## Measurement

Command: `make benchmark-mz037-incremental-top-k`

Workload: 10,000 active rows, `K=20`, and repeated arbitrary row replacement.
The rebuild baseline sorts the full relation and materializes equivalent
Top-K transition rows on every update. The incremental path seeds the same
relation outside the timer, then applies a delete-plus-insert replacement for
one keyed row. CPU: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64.

The benchmark reports Go `B/op` and `allocs/op` for the timed update path.
Those are transient allocations; the incremental operator also retains its
ordered index and the active rows, while the rebuild baseline retains only its
benchmark relation slice between updates.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Full rebuild, sort, and transition materialization | 76,354 | 1,088 | 7 | baseline |
| Incremental treap update and transition materialization | 1,736 | 1,212 | 7 | 43.98x faster |

The CPU win is the intended benefit for update-heavy bounded views. The
incremental path currently allocates 1.11x more transient bytes because it
preserves atomic validation, row ownership, and transition output; it is not
enabled implicitly and should be selected when update CPU dominates that
small memory cost.
Raw repeated samples are recorded in [BENCHMARK.md](BENCHMARK.md#mz-037-incremental-weighted-top-k).
