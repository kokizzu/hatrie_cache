# Typed JSON Subcolumns

CH-031 adds an opt-in columnar representation for frequently queried scalar
JSON paths. A source resolver can materialize a path such as `$.user.id` once
as a typed column and let SQL reuse it for `JSON_VALUE`, `JSON_EXISTS`, and
`JSON_QUERY` expressions. The ordinary row and map-subcolumn paths remain the
fallback, so existing sources and defaults do not pay for this representation.

## API

Build a scalar path column with `MaterializeJSONSubcolumn`:

```go
column, err := hatSql.MaterializeJSONSubcolumn("$.user.id", documents)
if err != nil {
	return err
}

batch := hatSql.ColumnarBatch{
	Rows: len(documents),
	JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
		{Field: "doc", Path: "$.user.id"}: column,
	},
}
```

Resolvers opt in through `ColumnarJSONSubcolumnSourceResolver`:

```go
ResolveSQLColumnarJSONSubcolumns(
	name string,
	key string,
	fields []string,
	paths []hatSql.ColumnarJSONSubcolumnRequest,
) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error)
```

Returning `available=false` keeps the established columnar, map-subcolumn, or
row execution path. A resolver can return both ordinary columns and typed JSON
subcolumns in one immutable batch.

## Representation

The representation uses one row-aligned payload for the inferred scalar kind:

| JSON values | Storage | Missing/null handling |
| --- | --- | --- |
| Integers | `[]int64` | `Present` and `Validity` bitmaps |
| Integers and floating-point values | `[]float64` | `Present` and `Validity` bitmaps |
| Strings | `[]string` | `Present` and `Validity` bitmaps |
| Booleans | bit-packed values | `Present` and `Validity` bitmaps |

`Present=0` means the path is absent. `Present=1` and `Validity=0` means the
path exists with JSON `null`. A valid scalar has both bits set. Objects and
arrays are rejected by the materializer and use the existing JSON path
fallback instead. Mixed incompatible scalar kinds are also rejected rather
than changing SQL type behavior.

The current feature is an in-memory columnar resolver contract. It does not
change the storage format, backup format, or wire protocol; callers decide
when to build and retain these reusable columns.

## Query Scope

The automatic path is intentionally narrow and predictable: a single-source
`CACHE` query with literal JSON paths and compatible projection, comparison, or
`IN` expressions. Qualified fields such as `src.doc` are supported. Unsupported
queries, unavailable columns, malformed columns, complex JSON values, and
non-scalar paths retain the exact existing evaluator.

## Measurement

Five `-benchtime=200ms` samples were collected on Linux `amd64` with an AMD
Ryzen 9 5950X using 4,096 JSON documents and a `JSON_VALUE` filter. The clean
row-source control and typed-subcolumn query return the same rows.

| Path | Median time | Heap/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing row-source JSON path | 9.30 ms | 7.28 MB | 90,138 | 1.00x |
| Typed JSON subcolumn query | 2.37 ms | 1.71 MB | 30,508 | 3.92x faster, 4.27x lower heap, 2.95x fewer allocations |
| One-time subcolumn materialization | 2.87 ms | 2.59 MB | 40,955 | paid once, reusable across reads |

The after-build ordinary-path control remained at 90,138 allocations/op, so the
lazy typed-column check does not add a meaningful allocation regression to
sources that do not opt in. Materialization is a deliberate upfront cost and
should be amortized across repeated queries or refreshes.

## Raw Output

Produced by `make benchmark-ch031-after-c242`:

```text
BenchmarkCH031JSONValueBaseline-32           	      25	   9189148 ns/op	 7283904 B/op	   90138 allocs/op
BenchmarkCH031JSONValueBaseline-32           	      25	   9392281 ns/op	 7283898 B/op	   90138 allocs/op
BenchmarkCH031JSONValueBaseline-32           	      24	   9300569 ns/op	 7283670 B/op	   90138 allocs/op
BenchmarkCH031JSONValueBaseline-32           	      26	   9608644 ns/op	 7283668 B/op	   90138 allocs/op
BenchmarkCH031JSONValueBaseline-32           	      26	   9087301 ns/op	 7283678 B/op	   90138 allocs/op
BenchmarkCH031JSONValueTypedSubcolumn-32     	      86	   2413446 ns/op	 1707570 B/op	   30508 allocs/op
BenchmarkCH031JSONValueTypedSubcolumn-32     	     100	   2381532 ns/op	 1707573 B/op	   30508 allocs/op
BenchmarkCH031JSONValueTypedSubcolumn-32     	     105	   2372477 ns/op	 1707566 B/op	   30508 allocs/op
BenchmarkCH031JSONValueTypedSubcolumn-32     	     100	   2339240 ns/op	 1707569 B/op	   30508 allocs/op
BenchmarkCH031JSONValueTypedSubcolumn-32     	     100	   2286530 ns/op	 1707568 B/op	   30508 allocs/op
BenchmarkCH031JSONSubcolumnMaterialize-32    	      94	   2889496 ns/op	 2589693 B/op	   40955 allocs/op
BenchmarkCH031JSONSubcolumnMaterialize-32    	     100	   2890709 ns/op	 2589696 B/op	   40955 allocs/op
BenchmarkCH031JSONSubcolumnMaterialize-32    	      84	   2872596 ns/op	 2589692 B/op	   40955 allocs/op
BenchmarkCH031JSONSubcolumnMaterialize-32    	     100	   2854735 ns/op	 2589690 B/op	   40955 allocs/op
BenchmarkCH031JSONSubcolumnMaterialize-32    	     100	   2851632 ns/op	 2589693 B/op	   40955 allocs/op
```

## M046 Ordered Top-N

M046 extends the same typed scalar path to bounded `ORDER BY
JSON_VALUE(...) LIMIT/OFFSET` queries. It keeps the existing row-source
fallback for unsupported query shapes or unavailable subcolumns. The measured
result is documented, including raw samples and the legacy control, in
[M046_JSON_SUBCOLUMN_TOPN.md](M046_JSON_SUBCOLUMN_TOPN.md).
