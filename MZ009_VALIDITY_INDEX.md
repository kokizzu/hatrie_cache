# MZ-009: VALID_AT Range Candidate Pushdown

Literal `VALID_AT(at, valid_from, valid_to)` predicates can now use the
existing `RangeIndexedSourceResolver` when the source provides a compatible
range index on `valid_from`. The optimizer requests candidates for the safe
lower bound `valid_from <= at`; the original `VALID_AT` expression is still
evaluated over those candidates, so the `valid_to` upper bound, NULL semantics,
and all timestamp validation remain exact.

The optimization applies to a single source with a literal timestamp and
field arguments, both qualified and unqualified. A missing or unavailable
range index returns `available=false` and preserves the existing full-source
path.

## Measurement

Target: `make benchmark-mz009-validity-index`.

Workload: 10,000 source rows, a prebuilt `valid_from` candidate list, and five
samples per mode on Linux/amd64 with an AMD Ryzen 9 5950X.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
BenchmarkMZ009ValidityIndexBaseline
3751798 4734739 20036
3891870 4734706 20036
3938877 4734704 20036
4012261 4734706 20036
4106779 4734704 20036

BenchmarkMZ009ValidityIndexIndexed
9533 6512 34
9680 6512 34
9321 6512 34
10272 6512 34
9931 6512 34
```

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Full-source `VALID_AT` scan | 3,938,877 | 4,734,706 | 20,036 | 1.00x |
| Range-index candidate pushdown | 9,680 | 6,512 | 34 | 406.9x faster, 727.2x lower heap, 589.3x fewer allocations |

The benchmark supplies a prebuilt candidate list; range-index build, update,
and storage costs are owned by the resolver and are not included. The feature
is therefore an execution-path improvement, not a claim that every source
should build a validity index.

## Verification

```text
make test-mz009-validity-index
make race-mz009-validity-index
make vet-mz009-validity-index
```
