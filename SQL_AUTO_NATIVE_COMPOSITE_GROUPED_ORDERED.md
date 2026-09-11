# Automatic Native Composite Grouped Top-N

M052y automatically selects the native batch executor for a narrow composite
grouped query shape:

- one ordinary `CACHE` or `KEYS` source;
- exactly two direct `GROUP BY` fields;
- grouped aggregate expressions supported by the native composite aggregator;
- optional `HAVING` that can be rewritten to grouped output fields and scalar
  comparisons;
- unqualified `ORDER BY` fields that resolve uniquely to selected output fields;
- finite `LIMIT`, with optional `OFFSET`.

The executor aggregates directly into composite group state, applies the
rewritten `HAVING` predicate, and keeps only the requested ordered page in a
bounded Top-N heap. It avoids materializing and sorting every group. The
existing executor remains the compatibility path for qualified, missing, or
ambiguous order fields, alias-only unsupported `HAVING`, `WITH TIES`, richer
SQL, and specialized resolver contracts.

Automatic selection is enabled by default. Set
`SQLQueryOptions.DisableNativeDataflow = true` to force the existing
materialized executor for compatibility or comparison.

## Correctness

`m052y_auto_native_composite_grouped_ordered_test.go` compares automatic and
forced-fallback columns and rows for a two-field grouped query with aggregate
`HAVING`, descending aggregate ordering, deterministic tie breakers, `LIMIT`,
and `OFFSET`. It also verifies that a qualified order field remains on the
fallback path.

## Benchmark

The benchmark uses the same compiled query and deterministic 20,000-row source
for both paths:

```sql
FROM CACHE('items') AS src
SELECT src.group_id, src.tier, COUNT(*) AS total, SUM(src.value) AS total_value
GROUP BY src.group_id, src.tier
HAVING COUNT(*) > 1
ORDER BY total DESC, group_id ASC, tier ASC
LIMIT 16 OFFSET 32
```

Five `-benchmem` samples were collected on `linux/amd64`, AMD Ryzen 9 5950X:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to fallback |
| --- | ---: | ---: | ---: | --- |
| Automatic native composite grouped Top-N | 4,841,388 | 3,549,388 | 4,294 | 4.53x faster, 7.16x less heap, 40.39x fewer allocations |
| Existing materialized fallback | 21,941,606 | 25,419,805 | 173,432 | control |

Raw baseline samples, before automatic selection (the automatic benchmark name
used the same fallback executor):

```text
automatic name before feature:
22255371 25419830 173433
22043094 25419696 173432
21887128 25419750 173432
21453861 25419902 173432
22309051 25419614 173432

fallback before feature:
21831853 25419795 173432
22029177 25419562 173432
22039760 25419550 173433
21604226 25420094 173433
23087590 25419906 173432
```

Raw final samples:

```text
automatic native:
4838062 3549410 4294
5137219 3549388 4294
4830210 3549388 4294
4841388 3549387 4294
4905845 3549387 4294

fallback:
22222557 25419676 173432
22243701 25419805 173432
21941606 25420093 173433
21253589 25419962 173432
21873843 25419638 173432
```

Run the reproducible checks with:

```text
make test-m052y-auto-native-composite-grouped-ordered
make benchmark-m052y-auto-native-composite-grouped-ordered
make test-race-m052y-auto-native-composite-grouped-ordered
make vet-m052y-auto-native-composite-grouped-ordered
```
