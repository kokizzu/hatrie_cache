# M-U10 Adaptive Logical Compaction

M-U10 adds an opt-in scheduler hint around
`hatSql.DifferentialTemporalJoin.Compact`. It does not start a worker, mutate
state from `ApplyLeft`/`ApplyRight`, or change the existing compaction safety
rule. The zero-value `DifferentialTemporalJoinDefinition` remains on the old
path because `CompactionPolicy` is nil by default.

## API

```go
policy := &hatSql.DifferentialTemporalJoinCompactionPolicy{
    MinUpdates:           4096,
    MaxStateBytes:        64 << 20,
    MaxFrontierAge:       5 * time.Minute,
    EstimatedBytesPerRow: 160,
}
join, err := hatSql.NewDifferentialTemporalJoin(
    hatSql.DifferentialTemporalJoinDefinition{
        MaxTimeDistance: 4,
        LeftKey:         func(row hatSql.SQLRow) string { return row["group"].(string) },
        RightKey:        func(row hatSql.SQLRow) string { return row["group"].(string) },
        CompactionPolicy: policy,
    },
)
if err != nil {
    return err
}

stats, hint, err := join.CompactIfNeeded(ctx, leftFrontier, rightFrontier, time.Now())
```

`CompactionRecommendation` is a read-only, allocation-free hint. It triggers
on the first threshold reached among estimated state bytes, non-zero updates
since the last successful compaction, and time since the last successful
frontier compaction. A caller can use the hint to schedule maintenance and
call `Compact` directly.

`CompactIfNeeded` uses the existing exact compactor when the context has no
cancellation channel, including `context.Background()`. For a cancellable
context it first collects all removable keys, checks cancellation, and only
then deletes and rebuilds indexes. Cancellation therefore cannot leave a
partially compacted join. Frontier regression and retraction semantics remain
the existing `DifferentialTemporalJoin` contract.

## Defaults And Limits

The policy is disabled when `CompactionPolicy` is nil. When a policy is
provided, zero fields use these defaults:

| Setting | Default |
| --- | ---: |
| `MinUpdates` | 1,024 non-zero updates |
| `MaxStateBytes` | 64 MiB estimated retained state |
| `MaxFrontierAge` | 1 minute |
| `EstimatedBytesPerRow` | 128 bytes |

The byte threshold is an estimate, not an allocator measurement. Set
`EstimatedBytesPerRow` from the row shape and include both sides of the join in
the budget. No threshold causes automatic compaction; the application still
chooses when to invoke the method and which frontiers are safe.

## Measurement

Commands:

```sh
make benchmark-mu10-baseline
make benchmark-mu10
make benchmark-mu10-cancellable
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture loads 1,024 rows
per side. The before compaction path is the existing M-U08 direct `Compact`;
the final nil-policy load is the same path after adding the opt-in fields.

| Workload | ns/op samples | Median ns/op | B/op | allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Before direct load | 3,575,277; 3,147,400; 2,883,524; 3,081,673; 3,009,750 | 3,081,673 | 2,979,086 | 13,457 | baseline |
| Final nil-policy load | 3,011,868; 3,201,711; 3,009,311; 2,985,649; 3,287,014 | 3,011,868 | 2,979,123 | 13,457 | within run variance |
| Final enabled-policy load | 3,174,139; 3,008,551; 3,461,424; 2,897,498; 3,131,926 | 3,131,926 | 2,979,152 | 13,458 | 1.04x vs nil-policy |
| Recommendation only | 77.87; 79.87; 79.58; 77.47; 75.84 | 77.87 | 0 | 0 | control-plane hint |
| Before direct compaction | 296,092; 298,008; 309,742; 312,651; 302,759 | 302,759 | 197,110 | 20 | baseline |
| Fast adaptive compaction | 327,521; 345,537; 329,721; 297,600; 303,535 | 327,521 | 197,139 | 1.08x CPU, +1 alloc |
| Cancellable adaptive compaction | 368,534; 387,513; 353,140; 375,300; 375,387 | 375,300 | 268,489 | 52 | 1.24x CPU, +71 KB |

The enabled policy adds one allocation and roughly 30 bytes per operation in
this load fixture. The fast adaptive compaction path is a small control-plane
cost because it reuses the existing compactor. The cancellable path pays for
two candidate-key slices and all-or-nothing mutation; use it when cancellation
or deadline handling matters, and use the fast path during a bounded
maintenance window.

