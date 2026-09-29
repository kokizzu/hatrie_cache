# ClickHouse-style grouped `WITH TOTALS`

`GROUP BY ... WITH TOTALS` returns the normal grouped rows in `QueryResult.Rows`
and one grand-total row in `QueryResult.Totals`. The totals row is separate from
the grouped result so existing consumers do not need to identify a sentinel
row inside `Rows`.

```sql
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS
ORDER BY src.category
```

The materialized response is equivalent to:

```json
{
  "columns": ["category", "total"],
  "rows": [
    {"category": "a", "total": 4},
    {"category": "b", "total": 2}
  ],
  "totals": [
    {"category": null, "total": 6}
  ]
}
```

Semantics:

- `Rows` keeps the ordinary grouping, ordering, `LIMIT`, and `HAVING` behavior.
- `Totals` contains one row and is computed from every row that survived the
  source and `WHERE` filters. `HAVING` does not remove the grand total.
- Expressions that are also grouping expressions are `null` in the totals row;
  aggregate expressions are evaluated over all filtered rows.
- `ORDER BY`, `LIMIT`, and `OFFSET` apply to `Rows`; they do not page or filter
  `Totals`.
- The field is omitted by JSON encoding when no totals were requested, so
  ordinary result payloads retain their existing shape.
- Streaming `ExecuteSQLQueryRows` rejects this syntax because its callback API
  has no separate totals channel. Use the materialized query API when totals
  are required.
- `GROUPING SETS`, `ROLLUP`, and `CUBE` combined with `WITH TOTALS` are rejected
  until their multiple subtotal semantics have an explicit result contract.
- `SELECT *` combined with `WITH TOTALS` is rejected; use explicit expressions
  so the totals row has stable columns.

The feature is materialized-only and defaults to the existing behavior because
the syntax is opt-in. It adds no totals allocation to ordinary grouped queries.

## Measurement

Five `-benchmem` samples were run on the same AMD Ryzen 9 5950X, linux/amd64,
using `make benchmark-chg05`.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Before: ordinary grouped query | 12600, 12259, 11468, 11691, 11271 | 11691 | 12304 | 81 | baseline |
| After: ordinary grouped query | 12088, 11814, 11875, 11732, 13217 | 11875 | 12320 | 81 | 1.02x |
| After: grouped query with totals | 12218, 12191, 12051, 11985, 12128 | 12128 | 12848 | 88 | 1.04x vs before |

The ordinary-query CPU median moved within normal short benchmark noise; its
allocation count stayed flat and bytes changed by 16 B. Producing the extra
totals row costs 528 B and 7 allocations in this five-row fixture, about 1.02x
the ordinary post-change CPU in this run. That cost is paid only by callers
that opt into `WITH TOTALS`.

Verification:

```text
make test-chg05
make race-chg05
make vet-chg05
make benchmark-chg05
```
