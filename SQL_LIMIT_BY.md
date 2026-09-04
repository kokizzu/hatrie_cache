# SQL `LIMIT BY`

`LIMIT n BY expression[, ...]` keeps at most `n` rows for each distinct value
of the expression list. It is useful for top-N results per tenant, region, or
other dimension without writing a window query.

```sql
FROM VALUES
  ('us', 10),
  ('eu', 8),
  ('us', 7),
  ('eu', 6),
  ('apac', 4)
AS scores(region, score)
SELECT region, score
ORDER BY score DESC
LIMIT 1 BY region;
```

Result:

| region | score |
| --- | ---: |
| us | 10 |
| eu | 8 |
| apac | 4 |

The cap is applied after filtering, grouping/projection, and `ORDER BY`. With
an `ORDER BY`, the highest-priority rows are retained; without one, source
order is retained. A global `LIMIT` may follow `LIMIT BY`, and `OFFSET` applies
to the resulting globally ordered rows:

```sql
FROM CACHE('events')
SELECT region, category, score
ORDER BY score DESC
LIMIT 2 BY region, category
LIMIT 100 OFFSET 20;
```

`NULL` values form one group. Text grouping follows the query collation, so
`SQLQueryOptions{Collation: SQLCollationUnicodeCI}` treats `"A"` and `"a"` as
the same group. Expressions may use source fields even when those fields are
not projected.

The ordinary query executor uses a bounded per-group Top-N heap for finite
ordered queries, then sorts only the retained candidates. Composite keys,
collations, prepared expressions, and external sort spill retain exact output
semantics. Streamed row callbacks use the same correct materialized fallback so
their ordering and resource-budget behavior match ordinary execution.

Existing queries are unchanged. `LIMIT BY` is opt-in, `FETCH` cannot be
combined with it, and malformed or duplicate clauses receive the normal SQL
diagnostic. Focused correctness and benchmark targets are:

```sh
make test-sql-limit-by
make benchmark-sql-limit-by-all
```

See the raw measurements in [BENCHMARK.md](BENCHMARK.md#sql-limit-by) and the
adoption decision in [ADOPTED_QUERY_ENGINE_IDEAS.md](ADOPTED_QUERY_ENGINE_IDEAS.md).
