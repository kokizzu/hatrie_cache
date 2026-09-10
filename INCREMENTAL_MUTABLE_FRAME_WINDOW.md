# Mutable Incremental Bounded Frame Windows

`hatSql.NewMutableIncrementalFrameWindow` extends the bounded
`ROWS BETWEEN N PRECEDING AND CURRENT ROW` frame API with exact row
`INSERT`, `UPDATE`, and `DELETE` maintenance. It is an opt-in Materialize-style
arrangement: the existing `NewIncrementalFrameWindow` constructor remains the
append-only, low-retention default.

## Usage

The definition is the same as the append-only frame API. A stable `RowKey` is
required because mutations identify rows by key. `OrderKey` should include a
unique deterministic tie-breaker when SQL `ORDER BY` values can be equal.

```go
definition := hatSql.IncrementalFrameWindowDefinition{
	Kind:           hatSql.IncrementalWindowFrameSumInt64,
	OutputColumn:   "running_amount",
	FramePreceding: 1,
	PartitionKey: func(row hatSql.Row) (string, error) {
		return row["account"].(string), nil
	},
	OrderKey: func(row hatSql.Row) (interface{}, error) {
		return row["sequence"], nil
	},
	RowKey: func(row hatSql.Row) (string, error) {
		return row["id"].(string), nil
	},
	ValueKey: func(row hatSql.Row) (interface{}, error) {
		return row["amount"], nil
	},
}

window, err := hatSql.NewMutableIncrementalFrameWindow(definition)
if err != nil {
	panic(err)
}

changes, err := window.Apply([]hatSql.IncrementalFrameWindowMutation{
	{
		Kind: hatSql.IncrementalFrameWindowInsert,
		Key:  "row-1",
		Row:  hatSql.Row{"id": "row-1", "account": "a", "sequence": int64(1), "amount": int64(10)},
	},
	{
		Kind: hatSql.IncrementalFrameWindowInsert,
		Key:  "row-2",
		Row:  hatSql.Row{"id": "row-2", "account": "a", "sequence": int64(2), "amount": int64(20)},
	},
})
if err != nil {
	panic(err)
}
_ = changes
```

`Apply` returns signed `DifferentialRow` values. For the two inserts above the
derived values are `10` and `30`. Updating `row-2` from `20` to `50` emits a
retraction for its old `30` value and an insertion for its new `60` value. Any
later rows whose frame includes `row-2` receive the same before/after pair.
Deleting a row emits a retraction for the deleted output and for every affected
downstream frame, followed by insertions for outputs that remain.

## Maintenance Behavior

- Ordered insert-only batches use the existing append path.
- An out-of-order insert, update, or delete builds candidates only for the
  affected partitions.
- Each affected partition is sorted, rebuilt, and validated before any state
  is published.
- Unchanged outputs are omitted; changed outputs are emitted as `-1` then `+1`
  differentials in deterministic key order.
- A failed callback, invalid value, overflow, duplicate, missing key, or
  mismatched row key leaves the complete window unchanged.
- All six bounded frame kinds are supported: `COUNT(*)`, `SUM(int64)`,
  `MIN(int64)`, `MAX(int64)`, `AVG(int64)`, and `COUNT(DISTINCT int64)`.

The mutable window retains base rows, derived outputs, and per-key partition
metadata. This increases resident memory compared with the append-only
constructor, but avoids a full-table recomputation for localized mutations.
The API is not concurrency-safe; serialize calls or protect the window at the
caller.

Peer-aware `RANGE` frames, dynamic frame bounds, arbitrary window expressions,
and automatic SQL planner selection remain outside this API.

## Measurement

The focused benchmark is `make benchmark-m065l-mutable-frame`. On Linux/amd64
with 1,024 rows, 16 partitions, a seven-row frame, and one update, the final
affected-partition path was `1.71x` faster than the same-run full recompute,
used `3.04x` fewer transient bytes, and made `1.61x` fewer allocations. The
benchmark reports transient `B/op`; the retained-row memory cost is an
intentional opt-in tradeoff. Raw samples and the rejected all-row-copy attempt
are recorded in [BENCHMARK.md](BENCHMARK.md#m065l-mutable-bounded-frame-windows).
