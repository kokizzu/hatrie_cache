# M213 Differential Consolidation

`hatSql.ConsolidateQuerySubscriptionDeltas` folds equal signed row updates
before a connector, projection, or other downstream consumer forwards them.
`ConsolidateQuerySubscriptionDeltaBatch` applies the same operation while
preserving batch metadata and copying the column list.

```go
folded, err := hatSql.ConsolidateQuerySubscriptionDeltaBatch(batch)
if err != nil {
	// reject the batch; overflow never returns partial output
}
send(folded)
```

Rows are identified with the same canonical SQL row identity used by
differential subscriptions, so map key order does not affect equality. The
input slice, row maps, and byte values are not mutated. Output rows own their
map and `[]byte` values. Equal updates retain the first surviving order;
zero-sum identities disappear, and a later reappearance is placed at the end.
Signed `int64` overflow returns `ErrQuerySubscriptionDeltaOverflow` without
returning partial output.

This is opt-in. Existing subscription production already consolidates its own
snapshot differences; this API is for callers that assemble additional
insert/delete streams or want an explicit boundary before forwarding. It does
not change the default subscription wire format.

## Benchmark

Measured on Linux/amd64, AMD Ryzen 9 5950X. The fixture contains 4,096 signed
updates for 128 distinct rows, with 31 inserts and one delete per row. The
baseline JSON path forwards every update; the candidate consolidates first and
then encodes the smaller batch. Five samples were used for each path.

| Path | Median ns/op | Median B/op | Median allocs/op | Wire bytes/op | Relative |
| --- | ---: | ---: | ---: | ---: | --- |
| Raw JSON forwarding | 3,578,215 | 1,287,959 | 28,676 | 344,901 | 1.00x |
| Consolidate then JSON | 6,947,828 | 2,913,662 | 38,325 | 11,031 | 1.94x CPU, 2.26x heap, 1.34x allocations; 31.27x smaller wire |

Raw samples:

```text
Raw JSON forwarding: 3578215, 3830969, 3903120, 3531997, 3564414 ns/op
Consolidate then JSON: 6947828, 7501028, 7335190, 6825268, 6852424 ns/op
```

Use this when transfer size or downstream update application dominates local
CPU and heap. Keep raw forwarding when the consumer is local and bandwidth is
not the bottleneck.

Repeat with:

```text
make benchmark-m213-differential-consolidation-baseline
make benchmark-m213-differential-consolidation
```

