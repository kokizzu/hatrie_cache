# CH-060 Bitmap Secondary-Index Union

This ClickHouse/Tarantool-inspired change removes the temporary ordinal `[]uint32`
materialization from the SQL secondary bitmap-index `OR` path. Each posting is
merged into the existing Roaring bitmap by visiting its array or dense bitset
containers, and the merged bitmap is then traversed directly into source-order
rows.

The `AND` path was measured separately and intentionally remains on its prior
ordinal-slice implementation: direct container filtering was about 1.6% slower
on the measured dense workload. This keeps the optimization limited to the
branch with a repeatable net win.

No bitmap format, public API, row ordering, source refresh, or clone semantics
changed. Unsupported predicates and non-indexed sources retain their existing
fallback behavior.

## Verification

`TestCH060BitmapSecondaryCombination` covers sparse and dense Roaring
containers for both `AND` and `OR`, including source-order row IDs. The related
CH-058 and CH-059 focused tests remain part of the verification target.

## Measurement

Five benchmark samples, 100,000 rows, AMD Ryzen 9 5950X:

| Path | Before median | After median | Improvement | Before bytes/op | After bytes/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `AND` | 7.363 ms | 8.457 ms | 0.87x (noise; path unchanged) | 8,916,211 | 8,916,217 | 50,007 | 50,007 |
| `OR` | 13.721 ms | 13.370 ms | 1.03x | 18,207,250 | 17,691,154 | 100,042 | 100,039 |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-060-bitmap-secondary-index-union).
