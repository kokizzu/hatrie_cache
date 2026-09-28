# M215 High-Churn Delta Join Maintenance

M215 adds `IncrementalJoin.ApplyConsolidated`, an opt-in maintenance path for
batches that contain high churn on one or both join inputs. It applies the
batch atomically and emits only the net difference between the join result
before and after the batch.

The existing `Apply` method is unchanged. Use it when downstream consumers
must observe every intermediate retraction and insertion. Use
`ApplyConsolidated` when the batch is a transaction-like update and only its
final result matters.

## Example

```go
join, err := hatSql.NewIncrementalJoin(hatSql.IncrementalJoinDefinition{
	LeftKey:  keyByAccount,
	RightKey: keyByAccount,
	Merge:    mergeAccountRows,
})
if err != nil {
	return err
}

netDelta, err := join.ApplyConsolidated([]hatSql.IncrementalJoinUpdate{
	{
		Side: hatSql.IncrementalJoinLeft,
		Row:  hatSql.DifferentialRow{Key: "account-1", Diff: -1},
	},
	{
		Side: hatSql.IncrementalJoinLeft,
		Row: hatSql.DifferentialRow{
			Key:  "account-1",
			Diff: 1,
			Row:  hatSql.Row{"account_id": "account-1", "region": "ap-southeast"},
		},
	},
})
```

The method retains signed multiplicity, row replacement, equality-key moves,
weighted pair counts, deterministic output ordering, row cloning, overflow
checks, and atomic failure behavior. It coalesces source updates first and
probes only source keys whose final state differs. A batch that inserts and
retracts the same source state produces no join probes or output.

## Measurement

Linux `amd64`, AMD Ryzen 9 5950X, `go test -benchmem -count=5 -benchtime=200ms`.
The fixture retains 512 counterpart rows in one equality bucket and applies
eight delete/insert churn cycles for one source key. The final source and join
state are unchanged.

| Path | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Existing `Apply` intermediate deltas | 13,762,744 | 7,766,384 | 41,037 | 1.00x |
| `ApplyConsolidated` net delta | 7,332 | 4,720 | 29 | 1,877.1x faster; 1,645.4x lower B/op; 1,415.1x fewer allocs |

This is not a drop-in CPU comparison for callers that require the 131,072
intermediate joined-row updates produced by the baseline. The gain comes from
explicitly choosing final-state semantics for high-churn batches; the default
path and its intermediate-output contract remain unchanged.

## Verification

The focused tests cover weighted replacements, both sides changing in one
batch, net-zero churn, deterministic output, row-state preservation, and
atomic rejection. The broader verification runs the focused race test, the
full `hatSql` package, and `go vet`:

```text
make test-m215-red
make test-m215-green
make benchmark-m215-baseline
make benchmark-m215-after
make verify-m215
```
