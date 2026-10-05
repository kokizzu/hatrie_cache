# `GROUP BY ALL`

Hatrie SQL supports the ClickHouse-style `GROUP BY ALL` shorthand. The parser
expands it into the existing grouped-query path after the complete select list
has been parsed.

```sql
SELECT region, product, SUM(amount) AS total
FROM sales
GROUP BY ALL
```

The query groups by `region` and `product`. Aggregate-only queries keep one
global group, while mixed scalar expressions contribute their non-aggregate
components. Window expressions are ignored because they are evaluated after
grouping. `SELECT * GROUP BY ALL` is rejected rather than guessing a schema
expansion.

Explicit `GROUP BY` behavior is unchanged. The implementation does not add a
second grouping operator or retained data structure; it reuses the existing
grouped SQL executor.

## Measurement

Command:

```sh
make benchmark-chu51-group-by-all
```

The permanent five-sample run measured the same 4,096-row workload for an
explicit group list and for `GROUP BY ALL`:

| form | median ns/op | B/op | allocs/op | relative time |
| --- | ---: | ---: | ---: | ---: |
| explicit `GROUP BY` | 942,615 | 687,158 | 152 | baseline |
| `GROUP BY ALL` | 944,304 | 687,141 | 152 | 1.00x |

The shorthand is therefore effectively neutral in this workload: it has the
same allocation count and almost identical runtime. This feature improves SQL
compatibility and query ergonomics; it is not a performance claim.
