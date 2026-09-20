# C248: Variant/JSON Subcolumn Projection

Status: adopted through the existing typed and dynamic JSON subcolumn paths.

The requested behavior, reading only referenced JSON paths instead of
re-decoding every document for every expression, is already implemented by
the `hatSql` columnar JSON subcolumn resolver and the opt-in `hatCache`
automatic materializer. The implementation is narrower than a general
variant engine: it stores reusable scalar paths and deliberately falls back
for objects, arrays, mixed incompatible types, unsupported SQL shapes, or
unavailable source layouts.

## Existing Layers

1. `JSONSubcolumnRegistry` interns repeated path strings into bounded process
   metadata IDs. It does not materialize values by itself.
2. `MaterializeJSONSubcolumn` creates a row-aligned typed scalar column with
   presence and validity bitmaps. The resolver accepts a list of requested
   paths, so unrelated JSON fields are not materialized for that query.
3. `JSONSubcolumnAutoMaterializer` promotes repeatedly requested paths after a
   bounded observation threshold. `HatTrie.ResolveSQLColumnarJSONSubcolumns`
   supplies the promoted columns only when the source generation still
   matches the cached data.

Supported scalar payloads are `int64`, `float64`, strings, and booleans.
Missing paths, JSON null, and valid values remain distinct through the
presence and validity bitmaps. Complex or incompatible values use the
existing correct row-oriented JSON path.

## Defaults And Scope

The dynamic materializer is disabled by default. Existing SQL and cache users
keep their current behavior unless they explicitly call
`ConfigureSQLJSONSubcolumnAutoMaterializer`. Even when enabled, only eligible
`CACHE` queries with literal, compatible paths use the promoted layout; all
other queries retain the established executor.

This is a projection optimization, not a storage or wire-format change. The
typed column is caller-owned/in-memory state and must be rebuilt or refreshed
when the source generation changes. The bounded limits and generation check
prevent stale values from being reused.

## Measurements

The existing CH031 five-sample benchmark used 4,096 JSON documents on an AMD
Ryzen 9 5950X, Linux amd64:

| Path | Median time | Heap/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Existing row-source JSON path | 9.30 ms | 7.28 MB | 90,138 | Baseline |
| Typed subcolumn query | 2.37 ms | 1.71 MB | 30,508 | 3.92x faster, 4.27x lower heap, 2.95x fewer allocations |
| One-time materialization | 2.87 ms | 2.59 MB | 40,955 | Amortized across reused reads |

The separate CH044 automatic-promotion benchmark reports a cached `Observe`
path at 28,925x faster than rematerializing the 4,096-row fixture with zero
allocations, and cached batch resolution at 6,589x faster with 752 B/op and
four allocations. These are opt-in cache-path measurements; they do not claim
that every JSON query receives the speedup.

Raw samples and reproduction commands remain in [BENCHMARK.md](BENCHMARK.md#c248-variantjson-subcolumn-projection),
[CH031_TYPED_JSON_SUBCOLUMNS.md](CH031_TYPED_JSON_SUBCOLUMNS.md), and
[CH044_JSON_DYNAMIC_SUBCOLUMNS.md](CH044_JSON_DYNAMIC_SUBCOLUMNS.md).

## Verification

The existing focused coverage includes path normalization, typed inference,
presence/null handling, unsupported-path fallback, source-generation
invalidation, bounded promotion, concurrent registry access, and SQL query
integration. The intended commands are:

```text
make test-json-subcolumns
make test-ch031-c242
make test-ch044
make test-race-json-subcolumns
make benchmark-ch031-after-c242
```

On the current feature worktree, the first command is presently blocked by
unrelated compile errors in `hat/hatSql/explain_arrangement.go` and
`hat/hatSql/m_u05_arrangement_recovery.go`; no C248 production code was
changed to mask those errors.
