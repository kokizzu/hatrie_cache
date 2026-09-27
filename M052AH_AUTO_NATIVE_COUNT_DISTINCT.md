# M052ah Automatic Native `COUNT(DISTINCT)`

## Summary

Ordinary row-resolver SQL queries using `COUNT(DISTINCT scalar)` now use the
existing native dataflow executor automatically. Grouped and global aggregates
reuse the existing typed integer/string distinct-key representation, so the
feature does not create formatted per-row keys or a second distinct-key model.

`SQLQueryOptions.DisableNativeDataflow` remains the explicit compatibility
fallback. Queries that reach the native runtime with an unsupported value type
return `ErrSQLNativeDataflowUnsupported` from direct native execution; automatic
execution treats that error as a fail-closed signal and reruns the general
executor, preserving the previous default behavior.

## Semantics and Limits

- `COUNT(DISTINCT field)` and `COUNT_OR_NULL(DISTINCT field)` are eligible for
  the native path when the argument is a scalar expression.
- Integer values are normalized through the existing integer key helper;
  strings use an exact typed string key.
- SQL NULL values are ignored, as required by `COUNT(DISTINCT ...)`.
- Duplicate values are retained only once per group.
- `SUM(DISTINCT ...)`, conditional distinct aggregates, star forms, custom
  functions, windowed expressions, and specialized source resolvers remain on
  their existing paths.
- Direct `CompileNativeDataflow` execution is deliberately fail-closed for
  unsupported runtime keys. Automatic query execution falls back instead of
  exposing that implementation limit as a new default error.

Example:

```go
query := "FROM CACHE('items') AS src SELECT src.region, COUNT(DISTINCT src.user_id) AS users GROUP BY src.region"
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{})
```

Set `DisableNativeDataflow: true` in `SQLQueryOptions` to force the existing
general executor for comparison, debugging, or a workload with known
unsupported runtime types.

## Benchmark

Command:

```text
make baseline-m052ah-auto-native-count-distinct
```

Host: Linux/amd64, AMD Ryzen 9 5950X. The fixture uses 8,192 rows, 128 groups,
and 64 repeated integer values. Each case uses five samples with
`-benchtime=100ms -benchmem`.

The baseline was captured before the implementation, when both automatic and
explicit fallback execution used the general executor. The post-change run
measures the automatic native path and the unchanged fallback in the same
process.

| Path | Median ns/op | B/op | Allocs/op | Improvement vs baseline fallback |
| --- | ---: | ---: | ---: | ---: |
| Baseline automatic (pre-change fallback) | 11,961,020 | 11,928,290 | 99,892 | 1.00x |
| Post-change automatic native | 1,898,469 | 1,371,958 | 8,770 | 6.31x CPU, 8.69x bytes, 11.39x allocs |
| Post-change explicit fallback | 11,984,532 | 11,928,431 | 99,893 | 1.00x |

Raw pre-change samples:

```text
automatic: 12140682 11658164 11985048 10867045 11165231 ns/op
fallback:  11447295 10422280 11656062 11301473 10919232 ns/op
```

Raw post-change samples:

```text
automatic native: 1784766 1935041 1913851 1811735 1898469 ns/op
explicit fallback: 11984532 13327045 11338830 11961020 13024746 ns/op
```

The native path is a large win for this duplicate-heavy grouped workload. The
tradeoff is a typed hash set per active group and a deliberately narrow runtime
key domain; unsupported automatic inputs pay one native attempt plus a general
execution retry, while direct native callers receive an explicit error.

Focused correctness and safety commands:

```text
make test-m052ah-auto-native-count-distinct
make verify-m052ah-auto-native-count-distinct
```
