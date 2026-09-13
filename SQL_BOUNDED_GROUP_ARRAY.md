# Bounded `GROUP_ARRAY`

`GROUP_ARRAY(value, limit)` is an opt-in memory bound for grouped value
collection. It retains the first `limit` values in source-row order after any
`FILTER` predicate is applied.

```sql
SELECT region,
       GROUP_ARRAY(user_id, 8) AS representative_users
FROM CACHE('events')
GROUP BY region
ORDER BY region;
```

`limit` must be a non-negative integer literal. `GROUP_ARRAY(value, 0)` returns
an empty array. The existing one-argument form remains unbounded and keeps its
previous NULL and ordering behavior. `ARRAY_AGG` and `GROUP_UNIQ_ARRAY` are
unchanged.

This is intended for representative samples, previews, and bounded API
responses. It is not a replacement for `LIMIT BY` when the requirement is to
limit result rows rather than values inside each aggregate.

## Measurement

The benchmark uses one 16,384-row group on Linux/amd64 with an AMD Ryzen 9
5950X and five samples with `-benchmem`.

| Workload | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Unbounded `GROUP_ARRAY(value)` | 11,848,019 | 21,591,538 | 81,984 | baseline |
| `GROUP_ARRAY(value, 16)` | 10,666,839 | 19,495,764 | 65,615 | 1.11x faster, 9.7% lower B/op, 20.0% fewer allocations |

With `FILTER`, the post-change control was 14,044,706 ns/op, 23,428,621
B/op, and 98,375 allocs/op; the bounded form measured 11,130,795 ns/op,
19,499,335 B/op, and 65,638 allocs/op. That is 1.26x faster, 16.8% lower
B/op, and 33.3% fewer allocations for the filtered workload.

The bounded form avoids evaluating rows after the limit is reached and avoids
materializing a full filtered-row slice. The tradeoff is intentional
truncation: callers must use the unbounded form when all grouped values are
required.

## Verification

```text
make test-ch041
make benchmark-ch041-after
```
