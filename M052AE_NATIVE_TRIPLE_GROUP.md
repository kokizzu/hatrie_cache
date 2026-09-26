# M052ae Native Three-Field GROUP BY

M052ae extends the opt-in native SQL dataflow executor to an unbounded
three-field direct `GROUP BY` over ordinary `CACHE`/`KEYS` rows.

```sql
FROM CACHE('items') AS src
SELECT src.region AS region,
       src.tier AS tier,
       src.channel AS channel,
       COUNT(*) AS total,
       SUM(src.value) AS total_value
GROUP BY src.region, src.tier, src.channel
```

The compiled native path uses a fixed comparable three-component key. Integer,
string, and `NULL` components preserve the existing native grouping semantics,
including first-seen group order and explicit rejection of unsupported runtime
key types. Automatic execution selects the path for ordinary row resolvers;
`DisableNativeDataflow: true` remains the explicit fallback.

The implementation deliberately remains fail-closed for four or more grouping
fields, grouped `HAVING`, grouped ordering, and bounded grouped output. Those
queries continue through the established executor rather than being silently
misclassified.

## Verification

```text
make test-m052ae-native-triple-group
make test-m052ae-sql-package
make race-m052ae-native-triple-group
make benchmark-m052ae-native-triple-group
```

The tests compare compiled native rows with ordinary execution, verify
automatic selection, exercise `NULL`, empty strings, integer and string keys,
check cancellation and unsupported runtime keys, and preserve rejection of
unsupported shapes.

## Benchmark

The paired benchmark uses 20,000 rows and about 1,536 groups on Linux/amd64
with an AMD Ryzen 9 5950X. The fallback explicitly disables native dataflow.
Five `-benchmem` samples produced a median of 23.010 ms/op for the fallback and
6.070 ms/op for native execution: **3.79x faster**, **4.39x lower B/op**, and
**50.0x fewer allocations**. Raw samples and calculation are in
[BENCHMARK.md](BENCHMARK.md#m052ae-native-three-field-group-by).
