# T-U26: Named Index Hints And Strategy Inspection

Status: adopted as an opt-in SQL capability.

## What changed

`hatSql.SQLIndexHint` now accepts an optional `Index` name. `FORCE` can use
that name when the resolver implements `hatSql.NamedIndexedSourceResolver`:

```go
options := hatSql.SQLQueryOptions{
	IndexHint: hatSql.SQLIndexHint{
		Source: "person",
		Field:  "region",
		Index:  "region_copy",
		Mode:   hatSql.SQLIndexHintForce,
	},
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, options)
```

Existing field-only hints remain compatible. An empty `Index` uses the old
`IndexedSourceResolver` path, so existing callers and automatic planning keep
their previous behavior.

`hatSchema.SQLResolverAdapter` and `MaterializedSource` support named equality
lookup for named functional, secondary, and covering indexes. Conditional
functional indexes are rejected for a named hint because the hint alone cannot
prove that the row predicate implies the index predicate. A missing named index
or a resolver without the named-index contract returns an explicit error for
`FORCE` instead of silently scanning.

Named `FORBID` is deliberately conservative: the current field-level resolver
contract cannot enumerate alternative named indexes, so it falls back to a
scan rather than risk using the forbidden index. Future resolver contracts can
add a planner-visible candidate list without changing this API.

## Strategy inspection

`EXPLAIN ANALYZE` already reports `INDEX CANDIDATES`, including selected and
rejected strategies with reasons. Together with named `FORCE`, this gives an
operator a way to inspect the planner and reproduce a particular embedded
index choice without making that choice the global default.

## Measured cost

The benchmark uses 128 rows and five samples per case on the same AMD Ryzen 9
5950X host. The ratio is `before / after`; values near `1.00x` are within
normal benchmark noise.

| Path | Before median | After median | Ratio | Heap | Allocs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default query | 17,068 ns/op, 22,304 B/op | 16,561 ns/op, 22,560 B/op | 1.03x | +256 B | 90 -> 90 |
| Existing field hint | 73,440 ns/op, 44,384 B/op | 70,886 ns/op, 44,659 B/op | 1.04x | +275 B | 825 -> 825 |
| Named hint | not available before | 70,786 ns/op, 44,790 B/op | 1.00x vs field control | +131 B vs field control | 827 |

This is an operational/planner capability, not a hot-path optimization. The
default path has no allocation-count increase and the small extra heap is the
cost of carrying the additional public index-name field through query options.
The raw runs are [baseline](T026_BENCHMARK_BASELINE_RAW.txt) and
[feature](T026_BENCHMARK_RAW.txt).

## Verification

- `make verify-t026`
- `make verify-t026-race`
- `make verify-t026-vet`
- `make verify-t026-sql`
- `make verify-t026-full-schema`
- `make benchmark-t026`

Focused tests cover named functional lookup, conditional-index rejection, and
validation that a named hint must specify `FORCE` or `FORBID`.
