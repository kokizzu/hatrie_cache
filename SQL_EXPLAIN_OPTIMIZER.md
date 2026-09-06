# SQL EXPLAIN Optimizer Diagnostics

`EXPLAIN ANALYZE` reports the optimizer's equality-index candidate decisions
on the `INDEX CANDIDATES` plan step. The existing `Node`, `Detail`, row counts,
and timing fields remain unchanged. Two additive fields make the decision
machine-readable:

- `Alternatives` lists each considered equality predicate in estimate order.
- `Notices` reports why a candidate was rejected or why adaptive execution
  selected a scan.

Each `ExplainAlternative` contains:

- `Expression`: normalized predicate text used by the plan.
- `EstimatedRows` and `EstimatedCost`: the candidate's integer estimates.
- `Selected`: `true` only for the chosen index candidate.
- `RejectedReason`: set for rejected candidates and omitted for a selected one.

Each `ExplainNotice` contains a stable `Code` and human-readable `Detail`.
Current codes are `optimizer_alternative_rejected` for an unavailable index and
`optimizer_scan_fallback` when adaptive history rejects an index probe.

Example:

```go
result, err := hatSql.ExecuteSQLQuery(
    "EXPLAIN ANALYZE FROM CACHE('orders') AS o WHERE o.status = 'open' AND o.region = 'us' SELECT o.id",
    resolver,
)
```

When the resolver exposes statistics but no usable index, the plan includes a
step equivalent to:

```json
{
  "node": "INDEX CANDIDATES",
  "alternatives": [
    {
      "expression": "o.status = \"open\"",
      "estimated_rows": 10,
      "estimated_cost": 11,
      "selected": false,
      "rejected_reason": "index unavailable"
    }
  ],
  "notices": [
    {
      "code": "optimizer_alternative_rejected",
      "detail": "o.status = \"open\": index unavailable"
    }
  ]
}
```

The metadata is emitted only for `EXPLAIN ANALYZE` decisions that already
produce an `INDEX CANDIDATES` step. Ordinary queries do not allocate or expose
these slices, and ordinary `EXPLAIN` continues to omit them.
