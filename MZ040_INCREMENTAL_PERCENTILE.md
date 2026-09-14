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
positive update for the same existing key, has an allocation-free validation
path. A complete replacement reuses its treap node; all other batches retain
the general atomic validation path.

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

Run:

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
