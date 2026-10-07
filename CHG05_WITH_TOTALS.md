# CH-G05: SQL `WITH TOTALS`

Hatrie SQL now supports ClickHouse-style `WITH TOTALS` for materialized grouped
queries.

## Syntax

```sql
SELECT region, sum(amount) AS total
FROM VALUES (...)
GROUP BY region WITH TOTALS
```

The normal grouped rows remain in `QueryResult.Rows`. The grand-total row is
returned separately in `QueryResult.Totals`; grouping dimensions are `NULL` in
that row. The internal grouping marker is removed before the result is exposed.

## Compatibility

- Queries without `WITH TOTALS` keep the existing result shape.
- The result is materialized so the totals row can be exposed separately.
- `ExecuteSQLQueryRows` rejects `WITH TOTALS` explicitly instead of silently
  discarding the totals row.
- `LIMIT`, `LIMIT BY`, set operations, `ROLLUP`, `CUBE`, and explicit grouping
  sets are rejected until their totals semantics are defined.
- Result caching is bypassed for totals queries because the totals section is
  part of the materialized result contract.

## Verification

Focused tests cover grouped rows, the grand total, the legacy result shape,
invalid ungrouped syntax, and the streaming API contract. The five-sample
benchmark compares the totals path with the existing explicit grouping-set
path.

Measured on the local benchmark workload (512 input rows, 32 groups):

| Path | Median time | Heap | Allocations |
| --- | ---: | ---: | ---: |
| Explicit grouping set | 579,709 ns/op | 901,605 B/op | 5,705 |
| `WITH TOTALS` | 603,287 ns/op | 906,506 B/op | 5,773 |

That run made `WITH TOTALS` 0.96x as fast as the equivalent explicit
grouping-set query, with 0.54% more heap and 1.19% more allocations. The
additional cost is the separate totals-row contract; the result is not
compared with a plain grouped query because that query does less work.

JSON serialization of the same result was 1,191 bytes without totals and
1,231 bytes with totals, a 40-byte increase for the extra row.
