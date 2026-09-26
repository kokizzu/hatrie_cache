# M052af Native Three-Field Grouped Ordered Top-N

M052af extends the native SQL dataflow path for three-field groups with a
bounded ordered result. It combines the fixed three-component group key from
M052ae with the existing Top-N heap:

```sql
FROM CACHE('items') AS src
SELECT src.region AS region,
       src.tier AS tier,
       src.channel AS channel,
       COUNT(*) AS total,
       SUM(src.value) AS total_value
GROUP BY src.region, src.tier, src.channel
HAVING COUNT(*) > 1
ORDER BY total_value DESC, region ASC
LIMIT 3 OFFSET 1
```

The compiled and automatic paths preserve ordinary SQL rows, first-seen group
order for stable ties, aggregate `HAVING` rewrites already supported by the
two-field path, alias-based order resolution, `NULL` ordering, and bounded
`LIMIT`/`OFFSET`. `DisableNativeDataflow: true` remains the explicit fallback.

The path is fail-closed for four or more group fields, `WITH TIES`, unselected
or expression-based order keys, unsupported `HAVING`, and other richer SQL
shapes. Those queries retain the established materialized executor.

## Verification

```text
make test-m052af-native-triple-grouped-ordered
make test-m052ae-sql-package
make race-m052af-native-triple-grouped-ordered
make benchmark-m052af-native-triple-grouped-ordered
```

Tests compare compiled and automatic native rows with ordinary execution,
cover `HAVING`, aliases, `NULL`, `LIMIT`/`OFFSET`, cancellation, and rejection
of unsupported shapes.

## Benchmark

The fixture uses 20,000 rows, 1,536 possible three-field groups, and a
`LIMIT 100 OFFSET 25` page on Linux/amd64 with an AMD Ryzen 9 5950X.

The pre-implementation fallback median was 24.640 ms/op, 27,930,992 B/op,
and 236,971 allocations/op. After implementation, the same-run fallback
control measured 25.418 ms/op, 27,931,116 B/op, and 236,971 allocations/op;
native measured 6.450 ms/op, 6,401,276 B/op, and 6,379 allocations/op. Against
the same-run control, native is **3.94x faster**, uses **4.36x fewer bytes**,
and performs **37.15x fewer allocations**. The pre-change and final raw
samples are recorded in [BENCHMARK.md](BENCHMARK.md#m052af-native-three-field-grouped-ordered-top-n).
