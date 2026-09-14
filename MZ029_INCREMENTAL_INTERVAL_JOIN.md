# Materialize-Style Incremental Interval Join

This adopts the interval-join maintenance idea listed as MZ-29 in
`INSPIRATION_BACKLOG.md`. The project’s separate `ENGINE_IDEAS.md` uses
MZ-029 for spillable arrangements, so that unrelated inventory entry remains
open.

`hatSql.IncrementalIntervalJoin` maintains an exact equi-inner join over
half-open validity intervals. It is an imported API for stream and CDC
consumers; it does not change the SQL planner or automatically attach itself
to a cache.

## What It Does

- Uses a caller-provided equality key and `[start, end)` interval on each side.
- Maintains signed source multiplicities, including positive inserts and
  negative retractions.
- Uses equality buckets with sorted interval entries and prefix maximum-end
  pruning, avoiding a full opposite-side scan for a small interval update.
- Emits only joined-row deltas affected by each source update.
- Applies a batch atomically: invalid intervals, missing rows, conflicting
  metadata, multiplicity errors, merge errors, and overflow leave maintained
  state unchanged.
- Supports same-key replacement after a source multiplicity reaches zero.
- Clones retained and returned rows, and returns deterministic full snapshots.

Overlap uses the standard half-open predicate:

```text
left.start < right.end && right.start < left.end
```

Intervals that only touch at an endpoint do not join. Source keys and equality
keys may not be empty or contain NUL bytes because NUL separates the left and
right source keys in the emitted joined key.

## Example

```go
join, err := hatSql.NewIncrementalIntervalJoin(hatSql.IncrementalIntervalJoinDefinition{
	LeftKey: func(row hatSql.Row) (string, error) {
		return row["account_id"].(string), nil
	},
	RightKey: func(row hatSql.Row) (string, error) {
		return row["account_id"].(string), nil
	},
	LeftInterval: func(row hatSql.Row) (int64, int64, error) {
		return row["valid_from"].(int64), row["valid_to"].(int64), nil
	},
	RightInterval: func(row hatSql.Row) (int64, int64, error) {
		return row["valid_from"].(int64), row["valid_to"].(int64), nil
	},
	Merge: func(left, right hatSql.Row) (hatSql.Row, error) {
		return hatSql.Row{
			"account_id": left["account_id"],
			"plan":       right["plan"],
		}, nil
	},
})
if err != nil {
	panic(err)
}

deltas, err := join.Apply([]hatSql.IncrementalIntervalJoinUpdate{
	{
		Side: hatSql.IncrementalIntervalJoinLeft,
		Row: hatSql.DifferentialRow{
			Key:  "account-1-left",
			Diff: 1,
			Row:  hatSql.Row{"account_id": "account-1", "valid_from": int64(10), "valid_to": int64(20)},
		},
	},
	{
		Side: hatSql.IncrementalIntervalJoinRight,
		Row: hatSql.DifferentialRow{
			Key:  "plan-1",
			Diff: 1,
			Row:  hatSql.Row{"account_id": "account-1", "plan": "pro", "valid_from": int64(15), "valid_to": int64(30)},
		},
	},
})
// deltas contains one positive row with key
// "account-1-left\\x00plan-1".
```

For a left multiplicity of `2` and a matching right multiplicity of `3`, the
joined multiplicity is `6`. A retraction emits the corresponding negative
product, allowing downstream differential state to update without rebuilding
the complete join.

## API

```go
type IncrementalIntervalJoinDefinition struct {
	LeftKey       IncrementalJoinKeyFunc
	RightKey      IncrementalJoinKeyFunc
	LeftInterval  IncrementalIntervalJoinIntervalFunc
	RightInterval IncrementalIntervalJoinIntervalFunc
	Merge         IncrementalJoinMergeFunc
}

type IncrementalIntervalJoinUpdate struct {
	Side IncrementalIntervalJoinSide
	Row  DifferentialRow
}

func NewIncrementalIntervalJoin(definition IncrementalIntervalJoinDefinition) (*IncrementalIntervalJoin, error)
func (join *IncrementalIntervalJoin) Apply(updates []IncrementalIntervalJoinUpdate) ([]DifferentialRow, error)
func (join *IncrementalIntervalJoin) Snapshot() ([]DifferentialRow, error)
func (join *IncrementalIntervalJoin) AllRows() ([]DifferentialRow, error)
```

The exported `ErrIncrementalIntervalJoin...` values can be checked with
`errors.Is`. `Apply` and `Snapshot` are intentionally not synchronized;
callers should serialize access just as they do for `IncrementalJoin`.

## Benchmark

The benchmark compares a full rebuild that groups and scans 10,000 source
rows per side with a warmed interval index processing a one-key replacement.
Index construction is outside the timed incremental loop. Results are medians
of five default one-second `go test -bench` samples.

| Path | Median time | Median bytes | Median allocations | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Full interval-join rebuild | 1,940,737 ns/op | 1,575,022 B/op | 803 allocs/op | 1.00x |
| Incremental indexed interval join | 1,747 ns/op | 2,048 B/op | 20 allocs/op | 1,110.9x time, 769.1x bytes, 40.2x allocations |

Raw samples:

```text
Full rebuild:
1965536 ns/op  1575033 B/op  803 allocs/op
1940737 ns/op  1575022 B/op  803 allocs/op
1943812 ns/op  1575021 B/op  803 allocs/op
1936473 ns/op  1575022 B/op  803 allocs/op
1884658 ns/op  1575021 B/op  803 allocs/op

Incremental:
1757 ns/op  2048 B/op  20 allocs/op
1737 ns/op  2048 B/op  20 allocs/op
3275 ns/op  2048 B/op  20 allocs/op
1746 ns/op  2048 B/op  20 allocs/op
1747 ns/op  2048 B/op  20 allocs/op
```

The maintained index retains source rows and per-key interval metadata, so
steady-state memory is proportional to retained input state. The measured
advantage applies when updates touch a small fraction of a large join;
`Snapshot` remains proportional to the complete joined result.

## Verification

Tests cover weighted overlap, endpoint semantics, retractions, replacements,
atomic failures, overflow, invalid sides and intervals, row cloning, and
2,000 deterministic randomized updates against a reference implementation.

```text
make verify-mz029-incremental-interval-join
make benchmark-mz029-incremental-interval-join
```
