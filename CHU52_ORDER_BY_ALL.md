# `ORDER BY ALL`

Hatrie SQL supports the ClickHouse-style `ORDER BY ALL` shorthand. It expands
the complete select list into the existing order-key path after the query has
been parsed.

```sql
SELECT region, amount
FROM sales
ORDER BY ALL DESC
```

The shorthand accepts one global `ASC` or `DESC` modifier and optional global
`NULLS FIRST` or `NULLS LAST`. Aggregate and window expressions are retained as
ordinary output order keys. `SELECT * ORDER BY ALL` is rejected because this
parser has no schema expansion for wildcards. Explicit `ORDER BY` behavior is
unchanged, and combining `ALL` with additional order expressions is rejected.

The implementation only creates the same `sqlOrder` entries that an explicit
list would create. It adds no retained state or alternate sorting operator.

## Measurement

Command:

```sh
make benchmark-chu52-order-by-all
```

The baseline was the explicit query before the shorthand implementation. The
post-change run compared both forms over 4,096 rows:

| form | median ns/op | B/op | allocs/op | comparison |
| --- | ---: | ---: | ---: | --- |
| explicit baseline | 10,430,957 | 3,872,044 | 20,512 | baseline |
| explicit after | 11,426,948 | 3,872,041 | 20,512 | run-to-run noise |
| `ORDER BY ALL` after | 10,757,908 | 3,872,024 | 20,512 | 0.94x explicit-after |

The allocation count and memory are unchanged. Because the explicit baseline
varied by more than the shorthand difference, this is recorded as a neutral
compatibility improvement, not a performance claim.
