# ClickHouse-Style Aggregate `If` Combinators

The SQL executor supports conditional aggregate names commonly used by
ClickHouse. Both the underscore form and the ClickHouse-compatible camel-case
form are accepted case-insensitively:

| Conditional aggregate | Accepted names |
| --- | --- |
| Count rows | `COUNT_IF(condition)`, `COUNTIF(condition)` |
| Sum values | `SUM_IF(value, condition)`, `SUMIF(value, condition)` |
| Average values | `AVG_IF(value, condition)`, `AVGIF(value, condition)` |
| Minimum values | `MIN_IF(value, condition)`, `MINIF(value, condition)` |
| Maximum values | `MAX_IF(value, condition)`, `MAXIF(value, condition)` |
| Value at largest ordering value | `ARGMAX_IF(value, order, condition)`, `ARGMAXIF(...)` |
| Value at smallest ordering value | `ARGMIN_IF(value, order, condition)`, `ARGMINIF(...)` |

Example:

```sql
FROM CACHE('events') AS event
SELECT
  countIf(event.active) AS active_events,
  sumIf(event.bytes, event.active) AS active_bytes
```

The condition uses the ordinary SQL truthiness rules; `NULL` does not qualify a
row. Numeric aggregates ignore NULL or non-numeric values using the same rules
as their unconditional counterparts. Conditional aggregates also work in
`GROUP BY` and `HAVING` queries.

The parser lowers these names to the existing aggregate-plus-`FILTER` state,
so no per-row combinator registry or extra retained data structure is needed.
Eligible global `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` queries use the existing constant
state stream path. Conditional arg-extreme aggregates use the established
general aggregate path; conditional window forms are rejected explicitly.

## Measurement

`make benchmark-ch038-aggregate-if` compared a 10,000-row `COUNT(*) FILTER`
control with `COUNT_IF` over the same source, using ten samples per variant.
The `COUNT_IF` median was 3,333,482 ns/op versus 3,422,337 ns/op for the
control, about 1.027x faster. It used 6,646,976 B/op versus 6,647,284 B/op
and 30,028 allocations/op versus 30,029.

The benchmark includes SQL parsing. The runtime plan is the same normalized
aggregate filter state, so the feature adds no retained data structure or
per-row state beyond the existing filter expression.
