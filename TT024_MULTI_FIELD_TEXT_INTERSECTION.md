# TT024: Multi-Field Text Index Intersection

## Adopted idea

Use multiple existing inverted text indexes as an intersection plan for a
pure `AND` of phrase or proximity predicates. This borrows the same general
principle as ClickHouse data-skipping/index-set intersection, Materialize
arrangement reuse, and Tarantool secondary-index filtering: reduce the row
candidate set before evaluating the complete expression.

The implementation is deliberately conservative:

- It applies only when every conjunct is `CONTAINS_PHRASE` or
  `CONTAINS_PROXIMITY` and every referenced field has a positional text index.
- Mixed expressions such as `text_predicate AND status = 'published'` keep the
  existing selective-index path.
- The SQL executor still evaluates every predicate on the returned candidates,
  so an index remains a candidate producer rather than a correctness authority.
- Existing single-field and multi-field `OR` plans are unchanged.

## Example

```sql
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.title, 'quick brown')
  AND CONTAINS_PHRASE(article.body, 'lazy fox')
SELECT article.id
ORDER BY article.id
```

The planner sends both predicates to the cache resolver. The resolver obtains
the same source snapshot for both indexes, intersects matching row positions,
and returns candidates in source order. The executor then rechecks the SQL
predicates.

## Benchmark

Command:

```text
make benchmark-tt024-text-intersection
```

The benchmark uses a fixed 10,000-row cache, a title index matching 10% of
rows, and a body index matching 1% of rows. Five samples are collected for
each path with `-benchmem` on an AMD Ryzen 9 5950X.

The control resolver exposes the existing single-index API but intentionally
does not expose the new intersection API. It therefore measures the old plan:
use one text index, then evaluate the second predicate over that candidate
set.

| Path | Run 1 ns/op | Run 2 ns/op | Run 3 ns/op | Run 4 ns/op | Run 5 ns/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Indexed intersection | 242,213 | 240,071 | 245,180 | 242,372 | 238,622 |
| Single-index control | 999,962 | 1,069,211 | 1,064,463 | 1,008,310 | 1,087,100 |

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Indexed intersection | 242,213 | 136,210 | 1,292 |
| Single-index control | 1,064,463 | 828,957 | 10,407 |

| Metric | Result |
| --- | ---: |
| Latency | 4.39x faster |
| Bytes allocated | 6.09x lower |
| Allocations | 8.05x fewer |

The gain comes from evaluating the second text predicate against the much
smaller first intersection instead of materializing and filtering the full
10%-candidate slice. This benchmark does not claim a service-level comparison
with Redis or Tarantool; it isolates the changed in-process SQL plan.

## Correctness coverage

`TestSQLTextPhraseIndexIntersectionAcrossFields` verifies direct resolver
results and the SQL path, including source-order candidates and exact phrase
semantics. Existing phrase, proximity, mixed-boolean, fallback, and snapshot
tests continue to cover the surrounding planner behavior.

Verification targets:

```text
make format-tt024-text-intersection
make test-tt024-text-intersection
make test-tt024-text-intersection-package
make race-tt024-text-intersection
make vet-tt024-text-intersection
make benchmark-tt024-text-intersection
```
