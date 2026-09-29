# M243 Arrangement Memory Metrics

M243 adds a read-only memory breakdown for typed-table aggregate arrangements,
inspired by Materialize's arrangement memory visibility. It keeps the existing
`EstimatedBytes` field and adds `KeyBytes`, `ValueBytes`, `TraceBytes`, and
`RetainedBytes` to `TypedTableAggregateArrangementStats`.

## Semantics

- `KeyBytes` estimates group-key maps, encoded group keys, dictionary/order
  metadata, and per-group key metadata.
- `ValueBytes` estimates aggregate state such as count/sum/min/max/distinct
  state. The sum of `KeyBytes` and `ValueBytes` is the legacy
  `EstimatedBytes` value, so existing consumers retain its meaning.
- `TraceBytes` estimates the retained source changefeed history shared by the
  arrangement's table. It uses a fixed 64-byte base per retained change so a
  stats scrape stays O(1) in history length; payload bytes are deliberately not
  scanned.
- `RetainedBytes` is the saturated sum of `EstimatedBytes` and `TraceBytes`.

All values are bounded estimates, not allocator readings. `Stats()` does not
change rows, compaction, checkpoints, or retention. Advancing
`CompactChangesThrough` reduces `TraceBytes` while key/value state remains
unchanged.

The fields are available from both `TypedTableAggregateArrangements.Stats()`
and an individual `TypedTableAggregateArrangement.Stats()` lease. The default
execution and storage paths do not collect or retain additional state until
the caller requests arrangement stats.

## Verification

```text
make verify-m243
make benchmark-m243
```

The focused regression test covers the breakdown, saturated total, changelog
compaction behavior, detached snapshots, lease lifecycle, and released-lease
errors.
