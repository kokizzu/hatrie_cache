# M052ag: Native Four-Field Grouped Ordered Top-N

M052ag extends the bounded native SQL dataflow path to exactly four grouping
fields. It is a ClickHouse-style small, fixed-key grouped state specialized for
the common `GROUP BY ... HAVING ... ORDER BY ... LIMIT/OFFSET` shape.

## Supported Shape

The automatic and explicit native paths accept a single ordinary `CACHE` or
`KEYS` source with:

- exactly four direct field expressions in `GROUP BY`;
- each grouped field projected exactly once;
- the existing scalar aggregate expressions, including `COUNT(*)` and
  `SUM(int64)`;
- scalar `WHERE` and the existing grouped `HAVING` rewrite;
- `ORDER BY` fields or unambiguous output aliases;
- finite `LIMIT` with optional `OFFSET`.

The grouping key stores four normalized integer, string, or NULL components.
Groups preserve first-seen order, while the existing bounded heap retains only
the requested ordered page.

## Fail-Closed Boundaries

Five or more grouping fields, `WITH TIES`, joins, windows, grouping sets,
custom functions, unsupported order expressions, ambiguous aliases, and
specialized resolver contracts keep the established executor. `CompileNativeDataflow`
and automatic selection both reject those shapes instead of changing semantics.

## Verification

```text
make test-m052ag-native-quad-grouped-ordered
make race-m052ag-native-quad-grouped-ordered
make test-m052ae-sql-package
make benchmark-m052ag-native-quad-grouped-ordered
```

The tests compare native rows with ordinary execution and the automatic path
with its explicit fallback, and cover NULL keys, aggregate `HAVING`,
`LIMIT`/`OFFSET`, cancellation, and the five-field rejection boundary.

## Benchmark

On Linux/amd64 with an AMD Ryzen 9 5950X, the five-sample median was 29.514
ms/op, 30,876,741 B/op, and 276,975 allocations/op for the fallback versus
7.822 ms/op, 7,534,180 B/op, and 6,379 allocations/op for native execution:
**3.77x faster, 4.10x lower bytes, and 43.42x fewer allocations**. Raw samples
and the calculation are recorded in
[BENCHMARK.md](BENCHMARK.md#m052ag-native-four-field-grouped-ordered-top-n).
