# M213: Equal Update Consolidation

## Decision

No new default forwarding hook is adopted.

The existing query subscription path already consolidates equal rows in
`querySubscriptionRowGroups` before `querySubscriptionDeltaBatchWithOrder`
creates a batch. The `UpsertChangefeed` and `DebeziumChangefeed` adapters also
already apply the M208 differential fold before consuming a batch. Adding a
second fold at the private channel enqueue boundary would therefore duplicate
work for the normal path.

## Measurement

Linux amd64, AMD Ryzen 9 5950X, five benchmark samples per variant. The
workload used 32 deltas with two columns.

| Path | Median ns/op | B/op | Allocs/op | Decision |
| --- | ---: | ---: | ---: | --- |
| Existing generated forwarding path | 27.90 | 0 | 0 | Keep |
| Generic fold on 32 unique unmarked deltas | 17,368 | 6,967 | 196 | Reject |
| Generic fold on 32 deltas / 16 unique rows | 18,973 | 6,967 | 196 | Reject as default |

The duplicate workload reduces the forwarded row records from 32 to 16, but
the fold cost is roughly 680x the existing forwarding cost before accounting
for downstream work. The optimization is only worthwhile when a future
operator can prove that duplicate deltas reach this boundary frequently and
the saved transfer or consumer work exceeds the folding cost.

## Follow-up Rule

If a future differential operator emits ungrouped batches through a public
boundary, add a focused test and benchmark that operator first. Consolidate at
that operator's natural ownership point instead of adding an unconditional
fold to the subscription producer.
