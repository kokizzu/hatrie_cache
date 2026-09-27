# CH037: Native Columnar ARRAY JOIN Element Fast Path

The ClickHouse-inspired CH037 columnar `ARRAY JOIN` executor already avoided
materializing source row maps. Its nested array values were still traversed
through `reflect.Value` for every row and element, even when a columnar source
provided the common `[]interface{}` representation.

## Change

The columnar executor now recognizes `[]interface{}` array values directly for
both output-capacity planning and element emission. Typed slices such as
`[]string` continue through the existing reflection path, preserving support
for callers that use typed array values. The ordinary row executor is
unchanged.

The fast path changes traversal only; it does not change NULL/empty behavior,
row limits, scalar validation, or output ordering.

## Benchmark

Command:

```text
make benchmark-ch037-native-array-fastpath
```

Environment: Linux amd64, AMD Ryzen 9 5950X, Go benchmark
`-benchtime=100ms -count=5`, 2,048 source rows, four `[]interface{}` elements
per row, 8,192 output rows.

| Path | ns/op samples | B/op samples | allocs/op samples |
| --- | --- | --- | --- |
| before, reflection columnar | 3425122, 3651280, 3924847, 3989243, 3655949 | 2822904, 2822929, 2822903, 2822939, 2822902 | 16401, 16401, 16401, 16401, 16401 |
| after, native slice columnar | 1713869, 1679419, 1625594, 1792588, 1842641 | 2822849, 2822855, 2822844, 2822846, 2822855 | 16400, 16400, 16400, 16400, 16400 |

Median comparison:

| Metric | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| columnar ARRAY JOIN time | 3,655,949 ns/op | 1,713,869 ns/op | 2.13x faster |
| heap bytes | 2,822,904 B/op | 2,822,849 B/op | 1.00x, 55 B lower |
| allocations | 16,401 | 16,400 | 1 allocation fewer |

The row fallback remains available for unsupported columnar sources, and typed
slice values retain reflection compatibility. The benchmark therefore measures
the intended native representation without removing a correctness fallback.

## Verification

```text
make test-ch037-native-array-fastpath
make test-ch037-native-array-fastpath-package
make race-ch037-native-array-fastpath
make vet-ch037-native-array-fastpath
make format-ch037-native-array-fastpath
```

Tests cover native and typed slice dispatch plus existing inner, left, empty,
NULL, fallback, and output-order behavior.
