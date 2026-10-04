# T-U26: Index Strategy Diagnostics In `EXPLAIN`

Regular `EXPLAIN` now exposes metadata-backed equality-index candidates for a
`CACHE` scan when the resolver supplies JSON index statistics. The scan step
reports:

- `alternatives`: each candidate expression, estimated rows, estimated cost,
  and whether it is the selected estimate;
- `notices`: structured rejection details for candidates that are not selected.

The diagnostics reuse the same bounded equality estimate and cost heuristic as
the runtime optimizer. They are read-only: `EXPLAIN` does not read source rows,
build an index, or change query execution. Candidate availability is based on
the metadata exposed by the resolver, so the result is a planning estimate,
not a guarantee that an application-owned runtime index is wired for every
candidate.

The existing `FORCE` and `FORBID` index-hint controls remain the execution
controls. When a hint is present, the diagnostic alternatives explain which
candidates are excluded by that hint. Queries without matching index metadata
retain the existing result shape and do not emit optimizer columns.

## Output Shape

For a query such as:

```sql
EXPLAIN FROM CACHE('orders') AS o
WHERE o.status = 'open' AND o.region = 'us'
SELECT o.id
```

the result includes `alternatives` and `notices` columns when the resolver
reports statistics for `status` and `region`. The plan still contains the
usual `SCAN`, `FILTER`, and `PROJECT` steps; the structured metadata is attached
to the `SCAN` row and is also available in `SQLQueryResult.Plan`.

## Cost

On the benchmark fixture, adding the metadata to indexed regular `EXPLAIN`
changed the median from 11,574 to 13,860 ns/op, 13,312 to 14,178 B/op, and
80 to 102 allocations/op. The no-index control changed from 5,216 to 5,316
ns/op and stayed at 8,256 B/op and 35 allocations/op. This cost applies only
when callers request regular `EXPLAIN` with matching statistics; ordinary query
execution is not changed.
