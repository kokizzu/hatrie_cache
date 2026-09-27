# CH-038 OrNull Native Dataflow

ClickHouse-style `COUNT_OR_NULL`, `SUM_OR_NULL`, `AVG_OR_NULL`, `MIN_OR_NULL`,
and `MAX_OR_NULL` already had correct generic and columnar implementations. The
automatic native row-resolver executor now recognizes the same aggregate names
and reuses the existing `sqlStreamAggregate` accumulator. The change is
opt-in through the existing automatic native-dataflow path; callers can still
force the general executor with `DisableNativeDataflow`.

`OrNull` returns `NULL` when no non-NULL input contributes to the aggregate.
`COUNT_OR_NULL(*)` returns `NULL` for an empty input and counts rows otherwise.
Grouped and global queries are covered, including groups containing only NULL
values.

## Benchmark

Command:

```text
make benchmark-ch038-ornull-native
```

Workload: five samples, `-benchtime=20x`, 20,000 input rows, a `WHERE` filter,
and all five `OrNull` aggregates. The pre-change automatic path rejected the
shape and used the general executor, so the pre-change fallback samples are
the representative baseline.

Raw pre-change baseline (`fallback`, ns/op, B/op, allocs/op):

```text
11266851  16632516  104038
 9953443  16631386  104036
10193763  16630840  104036
10245332  16630829  104036
 9899420  16630829  104036
```

Raw post-change automatic native path (`automatic`, ns/op, B/op, allocs/op):

```text
4145826  2248164  20029
4373649  2248165  20029
4494477  2248165  20029
4193680  2248170  20029
4491740  2248165  20029
```

Median comparison:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Pre-change general executor | 10,193,763 | 16,630,829 | 104,036 | 1.00x |
| Automatic native `OrNull` | 4,373,649 | 2,248,165 | 20,029 | 2.33x faster, 7.40x lower bytes, 5.19x fewer allocations |

The fallback path remains available and the benchmark's fallback variant stayed
within normal run-to-run noise after the change. No new per-row state or
allocation was added to the accumulator.

## Verification

```text
make test-ch038-ornull-native
make race-ch038-ornull-native
make vet-ch038-ornull-native
make test-ch038-ornull-native-package
```
