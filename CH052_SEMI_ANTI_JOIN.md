# CH-052: Semi and Anti Joins

This feature adds ClickHouse-style `SEMI JOIN` and `ANTI JOIN` operators to
the SQL executor.

## Semantics

```sql
SELECT l.id
FROM left_table AS l
LEFT SEMI JOIN right_table AS r ON l.key = r.key;

SELECT l.id
FROM left_table AS l
LEFT ANTI JOIN right_table AS r ON l.key = r.key;
```

`LEFT SEMI JOIN` keeps each left row whose join key exists on the right.
`LEFT ANTI JOIN` keeps each left row whose join key does not exist on the
right. Both operators preserve left-side duplicates, do not duplicate output
for duplicate right keys, and do not add right-side columns to the result.

The implementation accepts the equivalent bare forms `SEMI JOIN` and
`ANTI JOIN`. The join predicate must contain one equality comparison between
the left and right sources. A non-equality predicate is rejected instead of
silently falling back to a different semantic.

As with SQL equality joins, NULL keys do not match. Therefore a NULL left key
is removed by a semi join and retained by an anti join. A NULL right key never
matches a left key.

## Implementation

The executor resolves the right input once, inserts its distinct hashable keys
into a set, then probes that set for each left row. This avoids materializing
right-side payload columns and avoids producing one output row per matching
right duplicate.

The memory cost is `O(U)` for the distinct right-side keys, where `U` is the
number of unique non-NULL right keys. The operator is intentionally limited to
equality predicates so that this bounded hash-set strategy remains explicit.
The normal SQL row and join-work limits still apply.

## Benchmark

Command:

```text
make benchmark-ch052-semi-anti-join
```

Workload: 10,000 left keys, 4,096 right keys, integer equality membership,
five benchmark samples, `-benchmem`, on Linux/amd64 with an AMD Ryzen 9 5950X.
The naive baseline scans all right keys for every left key. The indexed path
builds a right-key set once and probes it.

| Path | Sample 1 ns/op | Sample 2 ns/op | Sample 3 ns/op | Sample 4 ns/op | Sample 5 ns/op | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Naive full scan | 6,520,217 | 6,026,491 | 6,859,327 | 6,697,807 | 13,335,139 | 6,697,807 | 32 | 1 |
| Indexed set probe | 140,777 | 150,489 | 146,277 | 156,514 | 151,705 | 150,489 | 0 | 0 |

Relative to the naive baseline, the indexed path is approximately **44.5x
faster** in this workload and removes the measured per-operation allocation.
The right-key set itself is setup memory, not included in the per-operation
allocation reported by the probe benchmark. Large or high-cardinality right
inputs therefore trade memory for the speedup.

The 13.3 ms naive sample shows normal benchmark variance from the shared
machine; the median and all raw samples are included above. This is a focused
algorithm benchmark, not a claim about end-to-end query latency.

## Verification

```text
make test-ch052-semi-anti-join
make test-ch052-hatsql
make race-ch052-semi-anti-join
make vet-ch052-semi-anti-join
```

The tests cover left duplicate preservation, right duplicate de-duplication,
semi and anti results, NULL behavior, and rejection of non-equality
predicates.
