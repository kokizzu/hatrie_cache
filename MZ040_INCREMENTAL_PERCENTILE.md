# MZ-040 Incremental Percentile

`hatSql.IncrementalPercentile` is an importable exact weighted order-statistics
operator inspired by Materialize arrangements. It maintains one row per
logical key and its signed multiplicity, so repeated percentile reads do not
sort the whole relation again.

## Usage

```go
percentile, err := hatSql.NewIncrementalPercentile(
	hatSql.IncrementalPercentileDefinition{
		OrderKey: func(row hatSql.Row) (interface{}, error) {
			return row["score"], nil
		},
	},
)
if err != nil {
	return err
}

err = percentile.Apply([]hatSql.DifferentialRow{
	{Key: "alice", Diff: 2, Row: hatSql.Row{"score": int64(10)}},
	{Key: "bob", Diff: 1, Row: hatSql.Row{"score": int64(20)}},
	{Key: "carol", Diff: 3, Row: hatSql.Row{"score": int64(30)}},
})
row, ok, err := percentile.Percentile(0.5)
// row.Key == "bob", ok == true
```

`Percentile` uses nearest-rank semantics. `p=0` selects the first row,
`p=1` selects the last row, and `0 < p < 1` selects rank
`ceil(p * TotalWeight())`. `Quantile` is an alias. An empty operator returns
`ok == false`; values outside `[0, 1]` and `NaN` return
`ErrIncrementalPercentileInvalid`.

Rows are ordered by the SQL `Compare` contract supplied by `OrderKey`, with
the differential key as a deterministic tie-breaker. `Apply` validates the
whole batch before mutation, rejects negative multiplicities, per-key counts
above `math.MaxInt64`, conflicting replacement payloads, and aggregate weight
overflow. Stored rows and returned rows are cloned, so callers may reuse or
mutate their input maps after the call.

The common two-record replacement shape, a negative update followed by a
positive update for the same existing key, reuses its treap node; all other
batches retain the general atomic validation path. A one-record `Apply` also
uses a dedicated path: existing-key increments and deletes avoid constructing
the batch pending map and prepared-update slice while retaining the same
validation and overflow rules.

## Costs And Scope

- Update: `O(B log N)` for a batch of `B` changes and `N` active keys.
- Percentile lookup: `O(log N)`.
- `TotalWeight`: `O(1)`.
- `Snapshot` and `AllRows`: `O(N)` and return cloned rows.
- Retained memory: one ordered treap node and one owned row payload per active
  logical key, plus the key map.

This is exact, not a bounded approximate percentile sketch. It is deliberately
an explicit imported operator: automatic SQL planner wiring, distributed
arrangement exchange, persistence, and mergeable sketch serialization remain
outside this feature.

## Benchmark

The historical replacement benchmark can be run with:

```text
make benchmark-mz040-incremental-percentile
```

The fixture has 10,000 rows, changes one row, and reads the 95th percentile.
The rebuild path sorts all rows per operation. The incremental path seeds the
same relation outside the timer, applies a delete-plus-insert replacement,
and performs an indexed percentile lookup. Linux `amd64`, AMD Ryzen 9 5950X,
five samples, `-benchtime=1s -benchmem`.

| Version/path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Before fast path: full sort | 74,453; 73,436; 74,688; 74,531; 74,524 | 74,524 | 104 | 4 |
| Before fast path: incremental | 895.8; 862.3; 862.0; 881.8; 874.9 | 874.9 | 903 | 6 |
| After fast path: full sort | 74,475; 71,252; 71,465; 69,732; 69,611 | 71,252 | 104 | 4 |
| After fast path: incremental | 639.3; 637.8; 639.5; 651.0; 637.8 | 639.3 | 679 | 4 |

The final incremental path is about `111.5x` faster than the final rebuild
median. The replacement fast path itself improved incremental CPU by about
`1.37x`, reduced transient bytes by about `1.33x`, and removed two
allocations. The remaining transient bytes are about `6.5x` the one-shot
baseline because the operator clones the updated row and returns an isolated
row while retaining the ordered state. Use it for repeated updates and
queries; use a one-shot sort for a single percentile over an otherwise unused
relation.

The focused single-update comparison can be run with:

```text
make benchmark-mz040
```

It uses 10,000 seeded rows, applies one existing-key increment, and reads the
95th percentile. Five samples were collected with `-benchmem` on the same
Linux `amd64` AMD Ryzen 9 5950X host. The rebuild and two-record paths are
controls; only the single-update path is changed by this optimization.

| Path | Before raw ns/op | Before median | After raw ns/op | After median | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full sort rebuild | 81,493; 77,167; 80,148; 77,350; 82,641 | 80,148 | 79,624; 85,692; 88,422; 83,698; 79,391 | 83,698 | 104 | 104 | 4 | 4 |
| Two-record replacement | 750.0; 765.6; 785.3; 781.8; 776.0 | 776.0 | 733.6; 766.5; 743.0; 705.9; 712.0 | 733.6 | 679 | 679 | 4 | 4 |
| Single existing-key update | 462.8; 474.4; 456.9; 479.0; 486.1 | 474.4 | 339.0; 334.5; 332.9; 330.9; 333.2 | 333.2 | 400 | 336 | 3 | 2 |

The single-update path is about `1.42x` faster, uses `1.19x` fewer transient
bytes, and removes one allocation. The two-record control is about `1.06x`
faster with unchanged memory, which is treated as noise-level rather than a
separate claimed improvement. The full-sort control is also unchanged within
normal benchmark variance.
