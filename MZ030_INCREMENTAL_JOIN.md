# MZ-030 Incremental Differential Join

`hatSql.IncrementalJoin` maintains an exact inner join as signed differential
updates arrive. It is a reusable imported API for stream and CDC consumers;
it does not change the SQL planner or automatically attach itself to a cache.

## What It Does

- Maintains one keyed multiset on each side of an inner join.
- Computes only the joined-row deltas affected by each update batch.
- Supports positive inserts and negative retractions through `Diff`.
- Treats a source key as one stable row identity. A key may be retracted and
  replaced atomically in the same batch.
- Applies a batch atomically: invalid rows, conflicting replacements, and
  checked-multiplicity overflow leave the prior state unchanged.
- Provides `Snapshot`/`AllRows` for a deterministic exact current result.
- Clones retained rows and returned rows so caller-owned maps cannot mutate
  maintained state.

The join is an exact equi-inner join. Each side supplies a key function, and
the merge function constructs the output row. Source keys and join keys may
not be empty or contain NUL bytes because NUL is reserved for the joined-row
identity separator.

## Example

```go
join, err := hatSql.NewIncrementalJoin(hatSql.IncrementalJoinDefinition{
	LeftKey: func(row hatSql.Row) (string, error) {
		return row["account_id"].(string), nil
	},
	RightKey: func(row hatSql.Row) (string, error) {
		return row["account_id"].(string), nil
	},
	Merge: func(left, right hatSql.Row) (hatSql.Row, error) {
		return hatSql.Row{
			"account_id": left["account_id"],
			"name":       right["name"],
		}, nil
	},
})
if err != nil {
	panic(err)
}

deltas, err := join.Apply([]hatSql.IncrementalJoinUpdate{
	{
		Side: hatSql.IncrementalJoinLeft,
		Row: hatSql.DifferentialRow{
			Key:  "account-1-left",
			Diff: 1,
			Row:  hatSql.Row{"account_id": "account-1"},
		},
	},
	{
		Side: hatSql.IncrementalJoinRight,
		Row: hatSql.DifferentialRow{
			Key:  "account-1-right",
			Diff: 1,
			Row:  hatSql.Row{"account_id": "account-1", "name": "Ada"},
		},
	},
})
// deltas contains one positive joined row with key
// "account-1-left\\x00account-1-right".
```

For a left row with multiplicity `2` and a right row with multiplicity `3`,
the joined output has multiplicity `6`. Retraction and insertion deltas use
the same product rule, so downstream consumers can update their own
differential state without rebuilding the complete join.

The merge callback receives retained rows as read-only values and must return
an independent output row. `Apply` returns deltas in update/match order;
`Snapshot` returns the complete result in deterministic joined-key order.

## API

```go
type IncrementalJoinSide uint8

const (
	IncrementalJoinLeft IncrementalJoinSide = iota + 1
	IncrementalJoinRight
)

type IncrementalJoinDefinition struct {
	LeftKey  IncrementalJoinKeyFunc
	RightKey IncrementalJoinKeyFunc
	Merge    IncrementalJoinMergeFunc
}

type IncrementalJoinUpdate struct {
	Side IncrementalJoinSide
	Row  DifferentialRow
}

func NewIncrementalJoin(definition IncrementalJoinDefinition) (*IncrementalJoin, error)
func (join *IncrementalJoin) Apply(updates []IncrementalJoinUpdate) ([]DifferentialRow, error)
func (join *IncrementalJoin) Snapshot() ([]DifferentialRow, error)
func (join *IncrementalJoin) AllRows() ([]DifferentialRow, error)
```

Use the exported `ErrIncrementalJoin...` sentinel errors with `errors.Is` for
validation, invalid-side, row-conflict, and overflow handling.

## Performance

The benchmark compares a full 10,000-result rebuild with a two-record source
replacement that emits only the affected deltas. Each result is from a five-
sample `go test -bench` run with the default one-second benchmark time.

| Path | Median time | Median bytes | Median allocations | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Full join rebuild | 3,970,549 ns/op | 5,530,631 B/op | 60,034 allocs/op | 1.00x |
| Incremental differential join | 1,624 ns/op | 2,071 B/op | 15 allocs/op | 2,444.9x faster |

Raw samples:

```text
Full rebuild:
4111724  5530651 B/op  60034 allocs/op
3970549  5530631 B/op  60034 allocs/op
3993966  5530651 B/op  60034 allocs/op
3891086  5530631 B/op  60034 allocs/op
3959076  5530631 B/op  60034 allocs/op

Incremental:
1604  2071 B/op  15 allocs/op
1655  2071 B/op  15 allocs/op
1652  2071 B/op  15 allocs/op
1619  2071 B/op  15 allocs/op
1624  2071 B/op  15 allocs/op
```

The incremental path retains both keyed source sides and affected rows, so
its steady-state memory is proportional to retained source keys rather than
to only the output delta. The large win applies when a small update changes a
small fraction of a large join; a full `Snapshot` still costs proportional to
the complete result and is the appropriate operation when all rows are
needed.

## Verification

The focused tests cover weighted multiplicities, retractions, same-key
replacement, atomic failures, overflow, row cloning, deterministic snapshots,
and 1,000 randomized updates against a reference implementation. Run:

```text
make verify-mz030-incremental-join
make benchmark-mz030-incremental-join
```
