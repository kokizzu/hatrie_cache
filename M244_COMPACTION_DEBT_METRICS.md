# M244 Compaction Debt Metrics

This adopts the Materialize idea of exposing compaction debt against a logical
frontier without changing query execution or automatic compaction.

`hatSql.TypedTableAggregateArrangements.CompactionStats()` returns a read-only
`TypedTableCompactionStats` value with:

- `LogicalFrontier`: the typed table's current change sequence;
- `CompactedThrough`: the oldest sequence already discarded from the table
  changefeed; and
- `CompactionDebt`: the saturated difference between those two frontiers.

The same method is available on `TypedTableAggregateArrangement` for callers
holding one arrangement lease. The value is a sequence count, not a byte
estimate, and it is separate from arrangement hydration lag (`SourceSequence -
Checkpoint`). A zero debt means the changefeed frontier is already compacted;
it does not mean that an arrangement has no retained group state.

The API is observational and disabled-by-default in the sense that it does not
start compaction, pin history, or change storage. It also does not add fields
to the existing `Stats()` return path, preserving its allocation shape. The
dedicated metric call measured `0 B/op` and `0 allocs/op`.

## Verification

Focused correctness, the full `hatSql` package, and the focused race test cover
the metric. Run the reproducible before/after benchmark with:

```text
make benchmark-m244
```

The benchmark uses a clean parent worktree, one CPU, seven samples for the
existing stats path, and five samples for the new metric call. Temporary
worktrees and Go caches are removed on exit.
