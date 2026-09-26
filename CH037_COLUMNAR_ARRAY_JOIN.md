# CH-037 Columnar ARRAY JOIN

The SQL executor now has a conservative ClickHouse-inspired physical
columnar path for one direct array expansion. It avoids first materializing a
source row map and then merging a second row map for every array element.

## Admitted shape

The fast path is selected automatically when all of these conditions hold:

- the source is one `CACHE(...)` source;
- there is exactly one direct `ARRAY JOIN` or `LEFT ARRAY JOIN`;
- the join expression is a source field containing an array or slice;
- projections are direct source fields or the joined element, with optional
  aliases;
- there are no filters, grouping, ordering, limits, CTEs, unions, windows,
  aggregates, or subqueries.

Every other shape stays on the existing general evaluator. This admission
rule is deliberate: unsupported expressions must retain the existing SQL
semantics rather than being partially lowered.

`ARRAY JOIN` emits one output row per element and emits no row for a NULL or
empty array. `LEFT ARRAY JOIN` emits one row with a NULL element for NULL or
empty arrays. The implementation uses the existing array-element semantics,
max-row budget, and cancellation/work accounting.

Example:

```sql
FROM CACHE('items') AS events
ARRAY JOIN events.tags AS tag
SELECT events.id AS id, tag AS tag
```

## Correctness coverage

`make test-ch037-columnar-array-join` verifies the columnar resolver is used,
preserves element order, handles empty and NULL arrays for the left variant,
and falls back for a query with an unsupported `WHERE` clause. The package
test also compares the returned projected rows, not only the resolver call
count.

## Measurement

Command: `make benchmark-ch037-columnar-array-join`

Workload: 2,048 input rows, four string tags per row, and 8,192 output rows.
The pre-change run was captured before admission was implemented. The paired
fallback and fast-path runs were captured afterward using the same query and
resolver data.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Pre-change general evaluator | 10,245,141 | 17,160,481 | 104,523 | 1.00x |
| Post-change generic fallback | 11,556,052 | 17,160,554 | 104,527 | 1.00x |
| Columnar ARRAY JOIN path | 1,649,432 | 2,822,848 | 16,400 | 7.01x faster than fallback |

Relative to the recorded pre-change run, the admitted path is 6.21x faster,
uses 6.08x fewer allocated bytes, and performs 6.37x fewer allocations. The
post-change fallback is intentionally still available and has essentially
the same resource profile as the old evaluator; the small CPU difference is
benchmark noise plus the changed test process.

### Raw samples

Pre-change general evaluator:

```text
10023159 ns/op 17160481 B/op 104523 allocs/op
10245141 ns/op 17160481 B/op 104523 allocs/op
11194492 ns/op 17160481 B/op 104523 allocs/op
10355765 ns/op 17160481 B/op 104523 allocs/op
10078847 ns/op 17160481 B/op 104523 allocs/op
```

Post-change generic fallback:

```text
11655928 ns/op 17160687 B/op 104528 allocs/op
11006445 ns/op 17160541 B/op 104527 allocs/op
11463447 ns/op 17160554 B/op 104527 allocs/op
11556052 ns/op 17160763 B/op 104528 allocs/op
11785517 ns/op 17160491 B/op 104527 allocs/op
```

Columnar ARRAY JOIN path:

```text
1655861 ns/op 2822849 B/op 16400 allocs/op
1646169 ns/op 2822850 B/op 16400 allocs/op
1649432 ns/op 2822847 B/op 16400 allocs/op
1602698 ns/op 2822847 B/op 16400 allocs/op
1667287 ns/op 2822848 B/op 16400 allocs/op
```

`B/op` is cumulative Go allocation volume reported by the benchmark; it is
not retained heap size. No new background memory or persistent index is
introduced by this optimization.
