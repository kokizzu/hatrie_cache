# M238 Explain Filter Pushdown And Arrangement Reuse

`EXPLAIN` now makes an existing execution optimization explicit. When a base
`WHERE` predicate is safe to evaluate before joins, its `FILTER` plan step
contains a machine-readable notice:

```json
{
  "code": "FILTER_PUSHDOWN",
  "detail": "predicate pushed before joins"
}
```

The notice is emitted only when `sqlCanPushBaseWhere` accepts the predicate.
Outer-join and subquery cases remain unmarked when the executor must preserve
their original evaluation boundary. `PREWHERE` is also marked as evaluated
during the source scan.

Arrangement reuse was already exposed through the public
`ExplainStep.Arrangements` field. Each `SQLArrangementMetadata` entry includes
`Reused`, `Key`, `Kind`, and bounded field metadata, so explain consumers can
distinguish a reused arrangement without a second redundant notice. Recommended
arrangements continue to use the existing `Recommended`, `MatchScore`, and
`Recommendation` fields.

## Verification And Tradeoff

The focused test checks both the pushdown notice and the reused arrangement
metadata, including the exact join-boundary wording. Existing explain plan
nodes and JSON fields remain compatible.

A five-sample, 500ms benchmark compared the parent commit with the current
tree on Linux amd64, AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Parent `EXPLAIN` | 34,844 | 16,730 | 127 |
| M238 `EXPLAIN` | 29,125 | 16,794 | 129 |

The CPU median was noisy across runs and is not claimed as a speedup. The
stable feature cost is two allocations and about 64 bytes per `EXPLAIN` result
for the structured notice. This is confined to diagnostics and does not add
work to ordinary query execution.

Run the checks with:

```text
make test-m238-explain
make race-m238-explain
make vet-m238-explain
make benchmark-m238-explain
```
