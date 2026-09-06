# SQL Collection Aggregates

Hatrie SQL supports collection-valued aggregates for grouped and global
queries. They preserve the source-row order presented to the aggregate.

## Functions

| Function | Arguments | Result | Semantics |
| --- | --- | --- | --- |
| `ARRAY_AGG(value)` | one | `[]interface{}` | Appends every value, including `NULL`. |
| `GROUP_ARRAY(value)` | one | `[]interface{}` | Alias for `ARRAY_AGG`. |
| `GROUP_UNIQ_ARRAY(value)` | one | `[]interface{}` | Keeps the first occurrence of each value, including at most one `NULL`. |
| `MAP_AGG(key, value)` | two | `map[interface{}]interface{}` | Inserts values by key; a duplicate key keeps the last source-row value. |

`MAP_AGG` rejects `NULL` keys and keys whose Go value is not comparable. An
empty input produces SQL `NULL`, matching the existing `SUM`/`MIN`/`MAX`
empty-input behavior. Values in a map may be `NULL`.

## Examples

Given rows:

```text
group | value | name
------+-------+-----------
a     | 1     | first
a     | 2     | second
a     | 2     | replacement
b     | 3     | third
```

Query:

```sql
SELECT group,
       ARRAY_AGG(value) AS values,
       GROUP_UNIQ_ARRAY(value) AS unique_values,
       MAP_AGG(CAST(value AS TEXT), name) AS names
FROM CACHE('items')
GROUP BY group
ORDER BY group;
```

Result:

```text
group | values       | unique_values | names
------+--------------+---------------+-----------------------------
a     | [1, 2, 2]     | [1, 2]        | {"1":"first", "2":"replacement"}
b     | [3]           | [3]           | {"3":"third"}
```

`FILTER` is evaluated before collection values are appended:

```sql
SELECT ARRAY_AGG(value) FILTER (WHERE value IS NOT NULL) AS present_values
FROM CACHE('items');
```

## Execution And Compatibility

These functions use the generic materialized aggregate evaluator. Existing
numeric streaming, indexed-group, columnar, spill, and ordered fast paths do
not claim collection aggregates, so adding them cannot approximate or change
those paths' results. `FILTER` uses the same aggregate-row filtering already
used by `COUNT`, `SUM`, and the approximate aggregates.

## Benchmark

The focused benchmark uses 1,024 rows, 64 groups, one `ARRAY_AGG`, and one
`MAP_AGG` per group on an AMD Ryzen 9 5950X:

```text
BenchmarkSQLAggregateCollections-32  985  1108042 ns/op  1311126 B/op  10087 allocs/op
```

Run it with:

```sh
make benchmark-aggregate-collections
```

The allocation count is expected for generic `map[string]interface{}` source
rows and collection results. This feature deliberately prioritizes complete
value and NULL semantics over pretending that collection aggregates are
constant-state numeric aggregates.
