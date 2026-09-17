# Arrangement Metadata In EXPLAIN

`hatSql` can expose the physical arrangements known by a source resolver in
regular `EXPLAIN` and `EXPLAIN PIPELINE` output. This follows the
Materialize-style idea of making reusable dataflow arrangements visible without
changing query execution.

## Optional Contract

Implement `SQLArrangementMetadataResolver` alongside the existing
`SQLSourceResolver`:

```go
type ArrangementResolver struct{}

func (ArrangementResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return rowsFor(name, key), nil
}

func (ArrangementResolver) ResolveSQLArrangementMetadata(name, key string) ([]hatSql.SQLArrangementMetadata, error) {
	return []hatSql.SQLArrangementMetadata{
		{Key: "events_by_id", Kind: "HASH", Reused: true, Cardinality: 120000, MemoryBytes: 7340032},
		{Key: "events_by_time", Kind: "ORDERED", Cardinality: 120000, MemoryBytes: 9437184},
	}, nil
}
```

The callback receives the source kind and source key, for example `CACHE` and
`events`. `CatalogResolver` forwards this optional contract to its application
resolver and does not call it for information-schema virtual sources.

## Output

When at least one operator has metadata, the structured plan includes an
`arrangements` array:

```json
{
  "node": "SCAN",
  "detail": "CACHE(\"events\")",
  "arrangements": [
    {"key": "events_by_id", "kind": "HASH", "reused": true, "cardinality": 120000, "memory_bytes": 7340032}
  ]
}
```

The tabular result adds an `arrangements` column only when it contains at least
one entry. Existing resolvers and plans therefore keep their current columns.
The metadata is attached to source scans and join operators, and is also
available on `SQLExplainStep` for callers that consume `Plan` directly.

## Safety And Cost

This is diagnostics only. It does not execute a source read, select an index,
or alter storage and wire formats. Resolver errors are ignored for explain
metadata, so an unavailable catalog cannot make a valid query fail.

Output is capped at 64 arrangements per operator. Each key and kind is capped
at 256 bytes. Returned strings and slices are copied before they are placed in a
plan or result row; dataflow, materialized-view, and result-cache plan clones
also deep-copy the arrangement slice.

The default path has no arrangement callback and keeps its existing explain
cost. With two entries, the measured overhead is 1.16x explain CPU, +552
allocated bytes per operation, and +13 allocations per operation. See the raw
five-run measurements in [BENCHMARK.md](BENCHMARK.md#mu-012-arrangement-explain).
