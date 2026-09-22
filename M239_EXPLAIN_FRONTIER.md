# M239 Explain Logical Timestamp And Frontier Requirements

`EXPLAIN` now exposes the temporal contract selected by `SQLQueryOptions` in
the existing machine-readable `ExplainStep.Notices` field. This makes a plan
self-describing without changing the execution or validation contract.

## Notices

The notices are attached to the first source `SCAN` step:

| Code | Detail | Meaning |
| --- | --- | --- |
| `LOGICAL_TIMESTAMP` | `as_of_frontier=<n>` | The query requests an immutable historical view at logical frontier `n` through `AsOfFrontier`. |
| `FRONTIER_REQUIREMENT` | `required_source_frontier=<n>` | Every non-local source must expose a ready frontier at least `n` through `RequireSourceFrontier` and `RequiredSourceFrontier`. |

Both notices can appear on one plan. `EXPLAIN ANALYZE` receives the same
annotations on its executed plan. A pipeline explain with a plan also receives
the annotations on its first plan step.

The options remain opt-in. With neither temporal option set, the helper
returns before touching the plan, and the default explain shape and allocation
profile are unchanged. Frontier validation still happens before source reads;
the notices describe the requirement and do not weaken it. `PlanSnapshot` is
also unchanged and remains the opt-in immutable snapshot representation for
callers that need a retained plan plus frontier pointers.

## Verification

The regression test covers regular `EXPLAIN`, `EXPLAIN ANALYZE`, both notices,
and the default no-notice path:

```text
make test-m239-explain
make race-m239-explain
make vet-m239-explain
```

## Benchmark

Five `-benchmem` samples were collected per path on Linux amd64, AMD Ryzen 9
5950X with `make benchmark-m239-explain`. The baseline is M238 (`a553a620`),
and the same temporal benchmark source was copied into the baseline worktree
so only the M239 implementation differed.

| Workload | M238 median ns/op | M239 median ns/op | M239 B/op | M239 allocs/op | M239 change |
| --- | ---: | ---: | ---: | ---: | --- |
| Default `EXPLAIN` | 26,924 | 22,246 | 16,793 | 129 | 0 B/op, 0 allocs/op; CPU is noisy |
| Opt-in temporal `EXPLAIN` | 9,914 | 8,560 | 9,312 | 33 | +120 B/op, +3 allocs/op |

Raw ns/op samples:

| Benchmark | M238 samples | M239 samples |
| --- | --- | --- |
| Default `EXPLAIN` | 27,630; 25,288; 25,306; 27,355; 26,924 | 19,771; 19,508; 22,246; 23,481; 23,858 |
| Opt-in temporal `EXPLAIN` | 9,914; 10,657; 11,226; 8,503; 9,498 | 9,680; 8,479; 8,395; 9,751; 8,560 |

The temporal path pays a small, bounded diagnostic cost for two stable string
details. The default path has no measured allocation or memory regression, so
the feature remains enabled only when the caller asks for temporal explain
metadata.
