# CH-029 Dictionary-Backed Join

`hat/hatSql` can execute an `INNER JOIN` or `LEFT JOIN` against an
`EXTERNAL(...)` source by probing a configured lookup arrangement for each
left-side join key. `hat/hatDictionary` provides the reusable adapter:
`NewSQLDictionaryLookupResolver` delegates ordinary fact-source resolution and
uses the bounded dictionary cache for the dimension side.

## Example

```go
dictionary, err := hatDictionary.New(dimensionSource, hatDictionary.Options{})
if err != nil {
	return err
}
resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(
	factResolver,
	dictionary,
	hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:  "countries",
		KeyField:   "code",
		ValueField: "name",
	},
)
if err != nil {
	return err
}

result, err := hatSql.ExecuteQueryParameters(ctx, `
FROM CACHE('orders') AS order_row
LEFT JOIN EXTERNAL('countries') AS country
  ON order_row.country = country.code
SELECT order_row.id, country.name`, resolver, nil, hatSql.QueryOptions{})
```

The adapter returns a two-field candidate row containing the join key and
dictionary value. The executor evaluates the complete `ON` expression again,
so a custom key encoder cannot weaken SQL equality semantics. Missing keys
produce no inner-join row and a NULL right side for a left join.

## Consistency And Fallback

The default key encoder handles strings, byte slices, booleans, and Go numeric
values. `SQLDictionaryLookupOptions.KeyEncoder` handles dates, composite keys,
or application-specific normalization. Set `ExpectedVersion` when every
lookup must use one `VersionedSource` snapshot; a mismatched source version
returns `ErrVersionMismatch` rather than mixing snapshots.

The adapter does not enumerate dictionary entries. A full external scan is
delegated only when `FullScan` is supplied, or when the base resolver already
implements `hatSql.ExternalSourceResolver`; otherwise unsupported query shapes
fail with `ErrSQLFullScanUnavailable`. This avoids silently turning a missing
lookup into an empty dimension.

## Scope

The point-lookup path applies to direct equality `INNER` and `LEFT` joins with
an `EXTERNAL` right source. Other join types and queries with right-side
pushdown requirements retain the existing source-resolution path. Normal
`CACHE` joins and all non-lookup resolvers are unchanged.

The lookup arrangement is useful when the fact side is much smaller than the
dimension or the dictionary is already warm. It adds one bounded cache lookup
per left row and retains dictionary memory; a full scan can be preferable when
the entire dimension is needed. The controlled benchmark in
[BENCHMARK.md#ch-029-dictionary-backed-join](BENCHMARK.md#ch-029-dictionary-backed-join)
uses 1,000 fact rows and 10,000 dimension rows: the warm dictionary path is
2.47x faster, with 65% less allocated memory and 64% fewer allocations.
