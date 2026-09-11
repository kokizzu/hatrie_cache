# SQL ASOF JOIN

Hatrie SQL supports a constrained ClickHouse-style `ASOF JOIN` for finding
the nearest right-hand row at, before, or after a left-hand row in time.

## Syntax

```sql
FROM CACHE('trades') AS l
ASOF [LEFT] JOIN CACHE('quotes') AS r
  ON l.symbol = r.symbol AND l.at >= r.at
SELECT l.at, r.price
ORDER BY l.at
```

The join accepts one equality predicate and one temporal inequality. The
equality partitions the right-hand rows into lookup buckets. The temporal
predicate selects one row from the matching bucket:

- `l.time >= r.time`: latest right row at or before the left row
- `l.time > r.time`: latest right row strictly before the left row
- `l.time <= r.time`: earliest right row at or after the left row
- `l.time < r.time`: earliest right row strictly after the left row

The operands may be written in either orientation. The comparison uses the
existing SQL comparison rules, so compatible numeric and temporal values can
be compared without a new storage type.

`ASOF LEFT JOIN` keeps every left row. When no right row qualifies, right-side
columns are returned as `NULL`. A regular `ASOF JOIN` keeps only rows with a
qualifying right-side match. Null equality keys and null temporal values never
match.

## Limits

The parser requires exactly one equality and exactly one temporal inequality
in the `ON` expression. Unsupported forms fail with a query error instead of
silently changing join semantics. Additional filters, `RIGHT JOIN`, and
`FULL JOIN` should be expressed separately or with a regular join when their
semantics are required.

The implementation uses a per-key right-side bucket, stable sorting, and
binary search for each left row. Equal timestamps retain source order; for a
non-strict `>=` or `<=` predicate the last eligible equal-time row is chosen.
The existing query row budget and join-work budget still apply.

## Benchmark Scope

The ASOF benchmark compares the nearest-match kernel only: a nested scan of
all right rows per left row versus a keyed bucket plus binary search. It does
not represent full SQL parsing, source resolution, projection, or network
latency. See the ASOF entry in [BENCHMARK.md](BENCHMARK.md) for the raw runs,
median, and the CPU/allocation tradeoff.
