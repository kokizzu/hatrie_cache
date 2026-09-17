# Ranked Incremental Top-K Changes

`hatSql.IncrementalTopK` maintains an exact weighted Top-K result. The
`ApplyWithRankChanges` method is an opt-in change stream for consumers that
need rank movement, not only membership changes.

## Example

```go
topK, err := hatSql.NewIncrementalTopK(hatSql.IncrementalTopKDefinition{
	K:          3,
	Descending: true,
	OrderKey: func(row hatSql.Row) (interface{}, error) {
		return row["score"], nil
	},
})
if err != nil {
	return err
}

_, err = topK.Apply([]hatSql.DifferentialRow{
	{Key: "a", Diff: 1, Row: hatSql.Row{"score": int64(10)}},
	{Key: "b", Diff: 1, Row: hatSql.Row{"score": int64(9)}},
	{Key: "c", Diff: 1, Row: hatSql.Row{"score": int64(8)}},
})
if err != nil {
	return err
}

changes, err := topK.ApplyWithRankChanges([]hatSql.DifferentialRow{
	{Key: "c", Diff: -1},
	{Key: "c", Diff: 1, Row: hatSql.Row{"score": int64(11)}},
})
if err != nil {
	return err
}
```

The replacement moves `c` from rank 3 to rank 1 and shifts the other rows:

| Key | Before rank | After rank | Before count | After count | Diff |
| --- | ---: | ---: | ---: | ---: | ---: |
| `a` | 1 | 2 | 1 | 1 | 0 |
| `b` | 2 | 3 | 1 | 1 | 0 |
| `c` | 3 | 1 | 1 | 1 | 0 |

`Diff` is `AfterCount - BeforeCount` in the bounded result. A rank-only
movement therefore has `Diff == 0`; consumers should use the rank fields for
ordering changes. A selected row entering the result has `BeforeRank == 0`
and a positive `Diff`. A row leaving has `AfterRank == 0` and a negative
`Diff`.

## Contract

- Ranks are one-based and identify the first occupied position for a row.
- Weighted rows occupy one position per selected multiplicity. Counts are
  capped at `K`; multiplicity outside the bounded result is not reported.
- `Row` is the current payload when a row remains or enters the result, and
  the previous payload when it leaves. Returned rows are cloned.
- Output is deterministic: changed rows that were selected before the batch
  are emitted in previous-result order, followed by rows newly selected after
  the batch in new-result order.
- The complete batch is validated before publishing state. Invalid keys,
  negative multiplicity, overflow, callback errors, and row conflicts leave the
  maintainer unchanged and return no changes.
- `Apply` keeps its existing membership-change behavior. The ranked method is
  opt-in and does not alter SQL plan selection or enable a default optimizer
  rule.
- The maintainer remains single-writer; callers sharing it between goroutines
  must provide synchronization.

## Cost

Updates retain the existing expected `O(log N)` treap maintenance. Ranked
reporting visits the bounded selections and compares them without a temporary
per-call rank map, using `O(K^2)` worst-case comparisons and `O(K)` result
capacity. This favors the intended small `K` use case and avoids allocating
rank maps on no-change updates.

The ranked method adds no persistent index or per-node rank field. It does
clone rows included in the returned change stream and allocates the ordinary
atomic update-validation state. See the fixed-workload measurements in
[BENCHMARK.md](BENCHMARK.md#mz-031-ranked-top-k-change-diffs).
