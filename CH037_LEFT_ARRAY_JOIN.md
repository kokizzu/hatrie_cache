# CH-037 Left Array Join

This is a partial ClickHouse-style `ARRAY JOIN` adoption. The SQL surface now
supports the opt-in form:

```sql
FROM CACHE('items')
LEFT ARRAY JOIN tags AS tag
SELECT id, tag
```

Ordinary `ARRAY JOIN` behavior is unchanged: an empty or `NULL` array emits no
row. `LEFT ARRAY JOIN` emits one row for those inputs and sets the array alias
to `NULL`; non-empty arrays preserve input order and emit one row per element.
Scalar expressions are still rejected, and the existing row/work limits apply
to the preserved rows. Nested-array traversal and physical nested-column
execution remain outside this incremental surface.

## Measurement

The benchmark uses 2,048 rows, four elements for non-empty arrays, and an empty
array every eighth row. Five samples were collected with `-benchmem`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Parent legacy emulation (`ARRAY JOIN` over `[NULL]`) | 13,117,353 | 15,957,277 | 95,265 | 1.000x |
| `LEFT ARRAY JOIN` | 13,069,621 | 15,957,290 | 95,265 | 1.004x faster |

The difference is within normal benchmark noise; the new behavior adds no
measurable memory or allocation cost. The unchanged inner control was also
stable at approximately 14.01 ms, 17.15 MB, and 104,481 allocations before
the change, versus approximately 13.91 ms, 17.15 MB, and 104,481 allocations
afterward.

## Verification

```text
make test-ch037-left-array-join
make race-ch037-left-array-join
make vet-ch037-left-array-join
make benchmark-ch037-left-array-join-before
make benchmark-ch037-left-array-join
```
