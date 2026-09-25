# MZ-035 Incremental Multiset

`hatSql.IncrementalMultiset` is an importable, opt-in state primitive for
maintaining exact differential multiplicities without rebuilding the entire
relation after every update. It is useful for callers that receive signed
changes from a source or dataflow and need an exact bag/multiset boundary
before applying another operator.

## Example

```go
multiset := hatSql.NewIncrementalMultiset()

changes, err := multiset.Apply([]hatSql.DifferentialRow{
	{Key: "user-1", Diff: 2, Row: hatSql.Row{"name": "Ada"}},
})
// changes[0].Diff == 2

changes, err = multiset.Apply([]hatSql.DifferentialRow{
	{Key: "user-1", Diff: -1},
})
// changes[0].Diff == -1; the remaining count is one

count, ok := multiset.Count("user-1")
// count == 1, ok == true
```

`Apply` returns the net signed change for each touched key. Positive updates
increase the exact count; negative updates retract that many copies. A key is
removed only when its count reaches zero. `Snapshot` and `AllRows` return one
row per active key with `Diff` equal to its full positive multiplicity, sorted
by key for deterministic replay.

## Correctness Rules

- Empty keys and zero-diff updates are rejected or ignored consistently with
  the differential API: empty keys return `ErrIncrementalMultisetInvalidKey`,
  and zero diffs are no-ops.
- A retraction larger than the active multiplicity returns
  `ErrIncrementalMultisetNegativeMultiplicity` and leaves the state unchanged.
- A non-nil row that differs from the active row for the same key returns
  `ErrIncrementalMultisetRowConflict`. A nil row on a later update means
  "reuse the stored row".
- Counts are bounded by `math.MaxInt64`, matching the signed `Diff` contract;
  overflow returns `ErrIncrementalMultisetOverflow` without changing state.
- Multi-row calls validate all updates before committing any state. A failed
  batch is therefore atomic.
- Input rows and returned rows are cloned at the state boundary. Callers may
  safely reuse or mutate their own row maps after a call.

The type is not internally synchronized. One caller must serialize `Apply`,
`Count`, and snapshot calls, or protect the instance with its own lock.

## Scope

This is a public building block, not an automatic change to existing SQL
execution. Existing differential operators and query plans retain their
current behavior. Planner integration for every bag-preserving operator,
durable multiset checkpoints, and distributed coordination remain future work.

## Measurement

The benchmark compares one update to a 1,024-row, 256-key fixture. The
baseline appends that update and calls `ConsolidateDifferentialRows`; the new
path seeds the same state once and applies the update incrementally. Five
samples were collected with:

```text
make benchmark-mz035-multiset-after
```

Median results from the matched run:

| Path | Time | Heap | Allocs | Relative to rebuild |
|---|---:|---:|---:|---:|
| Rebuild with `ConsolidateDifferentialRows` | 109,115 ns/op | 217,551 B/op | 520 | 1.0x |
| `IncrementalMultiset.Apply` | 90.96 ns/op | 48 B/op | 1 | 1,199x faster, 4,532x less heap, 520x fewer allocations |

The result is workload-specific: it measures a maintained state receiving a
single-key update, not a full rebuild from an external snapshot. The tradeoff
is retained in-memory state and caller-owned synchronization in exchange for
avoiding repeated relation-wide consolidation.
