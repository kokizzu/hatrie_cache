# CH-U49 Composite Index EXPLAIN Diagnostics

`EXPLAIN ANALYZE` now reports candidate and skipped-row counts for generic
composite JSON equality indexes and partial JSON indexes.

The feature is opt-in through `EXPLAIN ANALYZE`. Normal query execution keeps
the existing `SQLCompositeIndexedSourceResolver` path and does not call the
diagnostics resolver. Applications that wrap a source through
`hatSql.CatalogResolver` or the monitoring resolver receive the same optional
capability automatically.

## Reported fields

The existing `SQLIndexDiagnostics` payload is used:

- `kind`: `json_composite` or `json_partial`
- `field`: configured fields in index order, joined with commas
- `total_rows`: rows in the source snapshot
- `candidate_rows`: rows returned by the index before residual predicate checks
- `skipped_rows`: `total_rows - candidate_rows`
- `index_bytes`: bounded logical index-footprint estimate

The resolver is separate from candidate lookup. It refreshes the same index
snapshot and never changes which rows the query returns.

## Scope

Covered:

- `CreateSQLJSONCompositeIndex` equality predicates
- configured partial-index equality predicates
- direct `HatTrie` resolvers
- `CatalogResolver` and monitoring resolver forwarding
- source updates and index refreshes

Still separate work:

- covering-index diagnostics
- lower/text/multikey diagnostics
- composite range diagnostics
- typed composite range diagnostics

## Performance tradeoff

The benchmark uses four JSON rows and a two-field composite index on the same
machine, with five benchmark samples for each side. The normal query benchmark
does not use `EXPLAIN ANALYZE`.

| Path | Baseline ns/op | After ns/op | Change | Baseline B/op | After B/op | Baseline allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `EXPLAIN ANALYZE` composite equality | 35,704 | 37,799 | 1.059x, +5.9% | 24,118 | 24,915 | 267 | 283 |
| normal composite query | 12,763 | 12,905 | 1.011x, +1.1% | 10,152 | 10,152 | 63 | 63 |

The EXPLAIN cost is expected because it now refreshes and sizes the selected
index for the new report. Normal-query allocations are unchanged; the small
wall-time difference is within benchmark noise. The feature is retained because
the new diagnostics are available without changing normal-query behavior.
