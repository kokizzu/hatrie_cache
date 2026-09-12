# C220: QUALIFY

Hatrie SQL now supports ClickHouse-style `QUALIFY` filtering for queries that
select a window function. The predicate runs after window values and selected
custom functions are projected, and before `DISTINCT`, `ORDER BY`, and
`LIMIT`.

## Example

```sql
FROM VALUES ('a', 20), ('a', 10), ('a', 30), ('b', 5)
AS src(category, amount)
SELECT src.category,
       src.amount,
       ROW_NUMBER() OVER ranked AS row_number
WINDOW ranked AS (PARTITION BY src.category ORDER BY src.amount)
QUALIFY row_number <= 2
ORDER BY src.category, src.amount
```

The result contains the two lowest `amount` rows in each category. A
`QUALIFY` predicate can combine a selected window alias with source-qualified
fields, for example `QUALIFY row_number <= 2 AND src.amount >= 10`.

The current implementation requires the window function to be a selected
window expression and the predicate to reference its projected alias. A
window function written directly inside the `QUALIFY` predicate is rejected
with a diagnostic rather than being evaluated incorrectly. This keeps the
existing window executor and all default query behavior unchanged.

`ExecuteSQLQueryRows` uses the materialized query path for `QUALIFY`, ensuring
that streaming and columnar fast paths cannot emit rows before the predicate
has been applied.

## Measurement

Command:

```text
make benchmark-c220
```

The benchmark runs five samples at one second each on 1,024 rows across 16
partitions and compares `QUALIFY` with the equivalent nested-query plus
`WHERE` workaround. Values below are medians from the five samples on an AMD
Ryzen 9 5950X, Go amd64.

| Query shape | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `QUALIFY` | 1,091,077 | 928,448 | 5,336 |
| Nested query + `WHERE` | 1,145,589 | 1,062,258 | 5,422 |
| `QUALIFY / workaround` | 0.95x | 0.87x | 0.98x |

Raw `ns/op` samples:

| Query shape | Samples |
| --- | --- |
| `QUALIFY` | 1,091,077; 1,086,153; 1,094,953; 1,108,425; 1,012,946 |
| Nested query + `WHERE` | 1,061,202; 1,145,589; 1,097,228; 1,252,832; 1,272,495 |

The feature is primarily a capability and query-shape improvement. On this
run it was about 4.8% faster, used about 12.6% less measured allocation
memory, and performed 84 fewer allocations per operation than the workaround.

## Verification

```text
make test-c220
make format-c220
make benchmark-c220
```
