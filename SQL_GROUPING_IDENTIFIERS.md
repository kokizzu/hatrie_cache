# SQL Grouping Identifiers

Hatrie Cache supports ClickHouse-style grouping identifiers together with its
existing `GROUPING SETS`, `ROLLUP`, and `CUBE` syntax.

```sql
FROM orders
SELECT region,
       product,
       SUM(amount) AS total,
       GROUPING(region) AS region_grouped,
       GROUPING(product) AS product_grouped
GROUP BY GROUPING SETS ((region, product), (region), ())
```

`GROUPING(expression)` returns the integer `0` when the expression is present
in the current grouping set and `1` when that dimension was rolled up. This
lets callers distinguish a rollup-produced `NULL` from a real `NULL` value.
`ROLLUP` and `CUBE` use the same behavior. A normal `GROUP BY` returns `0`.

The parser expands grouping sets into the existing `UNION ALL` branches. The
identifier is folded to a literal during that rewrite, so it does not add
per-row execution state or map allocation. Only one grouping expression is
accepted per `GROUPING` call. Invalid arguments and use in `WHERE` or
`PREWHERE` fail explicitly.

This is a compatibility and correctness feature, not a claim that grouping
sets are faster than one ordinary grouping query. Returning identifier columns
adds result width; the measured cost is recorded in [BENCHMARK.md](BENCHMARK.md#ch-041-grouping-identifiers).
