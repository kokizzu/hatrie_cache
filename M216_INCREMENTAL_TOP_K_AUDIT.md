# M216 Incremental Top-K Audit

## Decision

M216 is already implemented by `hatSql.IncrementalTopK`. It maintains an
ordered treap and stable key map under signed weighted updates, then emits
only the rows entering or leaving the first `K` weighted positions. Replacement
and rank-change output is bounded by the selected result size.

The exact operator retains all active input entries in its ordered index. That
is required to identify the next replacement after a selected row leaves, so
this is not a claim of total `O(K)` memory. Splitting the state into selected
and remainder heaps would add update complexity without removing the required
non-selected input state.

## Correctness

Command:

```text
make test-m216-incremental-top-k-audit
```

Result: all focused `IncrementalTopK` and rank-change tests passed, including
weighted updates, stable ties, row cloning, atomic validation, randomized
reference comparison, and rank movement.

## Measurements

Five `-benchmem` samples ran on Linux amd64 on an AMD Ryzen 9 5950X. The
workload has 10,000 input rows, `K=20`, and changes one row per iteration.

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Rebuild and sort all 10k rows | 93,209 | 1,090 | 7 | baseline |
| Existing incremental Top-K | 2,778 | 1,212 | 7 | 33.5x faster; 11.2% higher transient bytes; same allocations |

Command:

```text
make benchmark-m216-incremental-top-k-audit
```

M216 is therefore closed as already covered. No runtime change was made.
