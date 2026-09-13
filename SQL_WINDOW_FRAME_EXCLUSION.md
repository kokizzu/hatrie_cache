# SQL window frame exclusion

Window frames can exclude the current row or its peers after the frame bounds
are resolved:

```sql
SELECT id,
       SUM(value) OVER (
         ORDER BY id
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         EXCLUDE CURRENT ROW
       ) AS prior_sum
FROM CACHE('events');
```

Supported modes are:

- `EXCLUDE CURRENT ROW`: remove only the row producing the window value.
- `EXCLUDE GROUP`: remove the current row and every peer with equal `ORDER BY`
  values.
- `EXCLUDE TIES`: remove peer rows but retain the current row.
- `EXCLUDE NO OTHERS`: retain the complete frame explicitly, which is the
  default.

Peer equality uses every `ORDER BY` expression. Without `ORDER BY`, all rows
are peers. Exclusion applies to windowed `SUM`, `AVG`, `MIN`, `MAX`,
`ARGMIN`, and `ARGMAX`; ranking and offset functions keep their normal
partition semantics.

The ordinary window path and default frame behavior are unchanged. The
exclusion check is only evaluated when an explicit exclusion mode is present.
