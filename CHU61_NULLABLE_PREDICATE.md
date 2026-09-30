# CHU61 Nullable Predicate Bitmap Fast Path

`IS NULL` and `IS NOT NULL` now use the validity bitmap already owned by
packed numeric, boolean, and dense nullable columnar layouts. The filter does
not decode values through `ColumnarBatch.Value` for each row. The general
evaluator remains the fallback for legacy plain columns, unsupported physical
layouts, malformed metadata, and wider expressions.

## Correctness

The focused tests cover:

- packed numeric columns with `IS NULL` and `IS NOT NULL`;
- all-valid and NULL-containing validity bitmaps;
- bit-packed boolean columns;
- dense nullable columns;
- legacy uncompressed columns;
- malformed bitmap lengths and trailing bits;
- row-source fallback behavior and exact result rows.

## Benchmark

Command:

```sh
make benchmark-chu61-nullable-predicate
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has 16,384
numeric rows, with every eighth row NULL, and selects the filtered value. The
before run disables only the CHU61 dispatch branch; the after run enables it.

| Workload | Before median ns/op | After median ns/op | CPU improvement | Before B/op | After B/op | Memory improvement | Before allocs/op | After allocs/op | Allocation improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `IS NULL` | 1,892,233 | 410,149 | 4.61x faster | 5,066,473 | 740,538 | 6.84x less | 4,187 | 4,126 | 1.01x fewer |
| `IS NOT NULL` | 4,119,345 | 2,695,862 | 1.53x faster | 9,572,536 | 5,246,613 | 1.82x less | 28,825 | 28,763 | 1.00x fewer |

Raw samples are `ns/op / B/op / allocs/op`:

```text
before IS NULL:     2025332/5066473/4187, 1892233/5066470/4187, 1883323/5066473/4187, 1889708/5066473/4187, 1906109/5066471/4187
after IS NULL:       408777/740542/4126,   407039/740540/4126,   410149/740538/4126,   425750/740538/4126,   414925/740538/4126
before IS NOT NULL: 4062053/9572573/28825, 4119345/9572523/28824, 4667681/9572559/28825, 4179787/9572525/28824, 4064148/9572536/28825
after IS NOT NULL:  2741576/5246626/28763, 2695862/5246613/28763, 2657061/5246613/28763, 2705178/5246613/28763, 2689598/5246613/28763
```

The fast path is deliberately narrow: it admits only direct field
`IS NULL`/`IS NOT NULL` predicates whose requested field has a validated
validity bitmap. NULL semantics are unchanged; NULL values never satisfy
ordinary comparison predicates, while these two predicates inspect only
presence.
