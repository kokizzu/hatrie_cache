# CH-045 Nested Map Subcolumn Pruning

Status: partially adopted.

The SQL columnar map planner previously accepted only one top-level object
member such as `$.country`. It now accepts any non-empty canonical SQL/JSON
path whose first segment is an object member, including nested members such as
`$.profile.country` and array traversal after an object root such as
`$.items[0].sku`.

The existing `ColumnarMapSubcolumnSourceResolver` contract already carries the
complete path. A resolver can therefore load only the requested nested path,
then return a `ColumnarMapColumn` whose values preserve the nested object or
array shape. The executor still evaluates the complete JSON path and retains
the distinction between missing and explicit `NULL` values.

Paths beginning with an array index, such as `$[0]`, continue to use ordinary
row execution because a map column has no object key to use as its first
lookup. Physical storage adapters and automatic nested-column materialization
are still caller-owned.

## Verification

The regression suite compares nested map-subcolumn execution with ordinary row
execution, checks the requested path, preserves missing and `NULL` semantics,
and verifies that root-array paths still fall back:

```text
make test-ch045-map-subcolumns
```

The benchmark temporarily restored the old one-segment planner condition for
the before measurements, then restored the implementation for the after
measurements. Results are recorded in `BENCHMARK.md`.
