# CH-43: ASOF Temporal Join

`hatSql.TemporalTable.AsOfJoin` performs an inner ASOF join between a batch of
left rows and versioned right-side rows. For each left row it returns the
latest right version for the same key whose timestamp is less than or equal to
the left timestamp.

## Example

```go
base := time.Unix(100, 0).UTC()
table := hatSql.NewTemporalTable()
table.Upsert("account-a", base, hatSql.Row{"tier": "free"})
table.Upsert("account-a", base.Add(10*time.Second), hatSql.Row{"tier": "paid"})

matches := table.AsOfJoin([]hatSql.AsOfJoinRow{
	{
		Key: "account-a",
		At:  base.Add(5 * time.Second),
		Row: hatSql.Row{"id": "a-5"},
	},
})
// matches[0].Right is {"tier": "free"}.
```

## Semantics

- Results retain the input order of `AsOfJoinRow` values.
- Rows without an earlier-or-equal right version are omitted, so the operation
  is an inner join.
- Equal timestamps use the latest `Upsert` at that timestamp.
- Left and right `Row` values are cloned. Mutating input or output rows does
  not mutate the temporal table.
- A nil table or empty input returns nil.
- The method does not parse SQL or change the SQL planner automatically. It is
  an explicit Go API over the existing `TemporalTable` primitive.
- The existing table is not made concurrency-safe by this API; coordinate
  concurrent `Upsert` and read access as with the other temporal methods.

For each key, nondecreasing left timestamps use a forward cursor after the
initial lookup. Out-of-order timestamps fall back to binary search. Cursor
state is retained only for keys that are revisited, and result capacity is
reserved from the input batch.

## Benchmark

The final paired benchmark used five samples with `-benchtime=250ms` on Linux/
amd64, AMD Ryzen 9 5950X. It compares the batch API with the same result
construction and cloning loop using a direct binary search. The fixture has
16,384 left rows, one key, 4,096 right versions, and cyclic timestamps that
exercise the out-of-order fallback.

| Operation | Baseline | ASOF join | Improvement | Memory | Allocs |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16,384-row batch | 12,446,920 ns/op | 11,778,269 ns/op | 1.06x faster | 12,320,840 vs 12,320,842 B/op | 65,539 vs 65,539 |

This workload shows a small CPU win and no meaningful allocation or memory
change. Monotonic per-key input is the workload that benefits most from the
forward cursor; the recorded mixed workload intentionally avoids claiming that
benefit for out-of-order batches.
