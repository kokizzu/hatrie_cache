# SQL ARRAY JOIN

`ARRAY JOIN` expands one source row into one output row per array element.
It is an inner expansion: a `NULL` or empty array produces no output row.

## Syntax

```sql
FROM CACHE('items')
ARRAY JOIN tags AS tag
SELECT id, tag;
```

The expression may evaluate to any Go array or slice value, including
`[]interface{}`, `[]string`, and `[]int64`. With `AS tag`, the element is
available through the `tag` column and the original source columns remain
available. Without an alias, a field expression uses its field name and the
expanded element shadows that unqualified field for the expanded row.

Example input:

```text
id | tags
---+----------
1  | [a, b]
2  | NULL
3  | [c]
```

Query and result:

```sql
FROM CACHE('items')
ARRAY JOIN tags AS tag
SELECT id, tag
ORDER BY id, tag;
```

```text
id | tag
---+----
1  | a
1  | b
3  | c
```

Scalar expressions are rejected with an error rather than silently producing
zero rows. The existing `MaxRows`, `MaxJoinWork`, cancellation, and result
limits apply to the expanded stream. Array expansion is deliberately routed
through the generic executor; numeric/indexed fast paths already exclude
queries with joins.

## Benchmark

The focused benchmark expands 1,024 rows with four string elements each on an
AMD Ryzen 9 5950X:

```text
BenchmarkSQLArrayJoin-32  223  5055584 ns/op  8778745 B/op  56350 allocs/op
```

Run it with:

```sh
make benchmark-array-join
```

This measures full SQL execution and result materialization, including the
expanded 4,096 output rows; it is not a standalone reflection-loop benchmark.
The implementation iterates the reflected slice directly instead of creating
a temporary interface slice for every input row.
