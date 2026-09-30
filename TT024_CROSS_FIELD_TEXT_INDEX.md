# TT-024 Cross-Field Positional Text Index Union

This is an opt-in extension of the TT-024 positional text index. An SQL
predicate such as:

```sql
FROM CACHE('docs') AS doc
WHERE CONTAINS_PHRASE(doc.title, 'quick brown')
   OR CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1)
SELECT doc.id
```

can now resolve candidates from both indexed fields before the normal SQL
executor evaluates the complete boolean expression.

## Contract

`hatSql.SQLTextProximityFieldQuery` associates a field with a phrase or
proximity query. `TextProximityMultiFieldUnionIndexedSourceResolver` is an
optional resolver contract:

```go
type SQLTextProximityFieldQuery struct {
    Field string
    Query SQLTextProximityQuery
}

type TextProximityMultiFieldUnionIndexedSourceResolver interface {
    ResolveSQLTextProximityMultiFieldUnionSource(
        name, key string,
        queries []SQLTextProximityFieldQuery,
    ) ([]Row, bool, error)
}
```

Hatrie and `hatSchema.SQLResolverAdapter` implement the contract. The
`MaterializedSource` implementation requires every requested field to have a
positional sidecar. It marks matching row positions once, emits rows in source
order, and leaves the final predicate evaluation to `hatSql`.

The existing same-field union path is unchanged. Queries with a missing
sidecar, unsupported resolver, mixed non-text boolean terms, or another
unsupported shape return `available=false` and use the ordinary full scan.
The feature adds no index or per-row cost until the caller explicitly builds
the text sidecars.

## Correctness

- Cross-field matches are deduplicated when one row satisfies multiple leaves.
- Phrase and ordered-proximity semantics are preserved by residual evaluation.
- Source order is preserved before later SQL `ORDER BY` processing.
- A partial set of field indexes never returns incomplete candidates.
- Hatrie and `MaterializedSource` both refresh sidecars against the same source
  snapshot before returning candidates.

## Benchmark

The fixture contains 20,000 rows and returns 40 rows. It has separate
positional indexes for `title` and `body`; five `-benchmem` samples ran on
Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Pre-change cross-field fallback | 29,321,564 | 29,985,684 | 280,117 | baseline |
| Post-change full-scan control | 31,904,627 | 30,006,259 | 280,119 | control |
| Post-change indexed cross-field union | 86,740 | 94,696 | 610 | 338x faster, 317x lower bytes, 459x fewer allocations than pre-change |

The indexed path pays for a row-mark bitmap and one candidate result slice, but
avoids scanning and evaluating 20,000 source rows. The all-fields-indexed
requirement is deliberate: using only one sidecar would be unsound for `OR`.
Raw samples are in
[`TT024_CROSS_FIELD_BENCHMARK_BASELINE_RAW.txt`](TT024_CROSS_FIELD_BENCHMARK_BASELINE_RAW.txt)
and [`TT024_CROSS_FIELD_BENCHMARK_RAW.txt`](TT024_CROSS_FIELD_BENCHMARK_RAW.txt).
