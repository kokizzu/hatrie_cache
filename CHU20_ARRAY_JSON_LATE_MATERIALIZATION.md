# CH-U20 Array/JSON Late Materialization

Status: implemented as an opt-in extension of the CH-031 typed JSON
subcolumn resolver.

## What Changed

`ColumnarJSONSubcolumn` now supports `ColumnarJSONSubcolumnJSON` for JSON
objects and arrays. The representation uses:

- `JSONOffsets`, one `uint32` boundary per row plus a final boundary.
- `JSONData`, one contiguous buffer containing the valid object/array bytes.
- The existing `Present` and `Validity` bitmaps for missing and JSON-null
  semantics.

Scalar paths retain the existing fixed-width or string representation. Array
indexes such as `$.items[1].sku` therefore continue to use the scalar fast
path, while `$.items` can retain the complete array without retaining a decoded
Go object per row.

`JSON_QUERY` decodes only the selected row's raw object or array. `JSON_EXISTS`
checks the presence bitmap without reading or decoding the payload. `JSON_VALUE`
continues to reject object and array results. Unavailable or unsupported
columnar resolvers still use the existing row evaluator.

## Correctness And Limits

- Row counts and offsets are validated before a columnar SQL scan runs.
- Every stored object/array must be valid JSON; malformed raw payloads are
  rejected with `ErrColumnarJSONSubcolumnInvalid`.
- Missing paths return `(nil, false)`; explicit JSON null returns `(nil, true)`.
- Mixed scalar and complex path values are rejected, preserving the existing
  typed-column fallback behavior.
- The contiguous payload uses `uint32` offsets, so one promoted complex path is
  limited to roughly 4 GiB of raw JSON. Existing row and retained-byte limits
  remain in force.
- Automatic promotion remains opt-in and in-memory. It does not persist the
  promoted column or change the wire/storage format.

## Benchmark

Five `-count=5` samples were collected on an AMD Ryzen 9 5950X Linux `amd64`
host using 256 documents. The row fallback compiles the path once, then parses
the complete source document for every row. The columnar case builds one
subcolumn, then reuses it for each operation.

| Operation | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Row fallback `JSON_QUERY` | 746,441 | 489,407 | 7,927 | 1.00x |
| Columnar late-materialized `JSON_QUERY` | 452,795 | 266,101 | 5,367 | 1.65x faster, 45.6% lower heap, 32.3% fewer allocations |
| Row fallback `JSON_EXISTS` | 710,116 | 489,407 | 7,927 | 1.00x |
| Columnar bitmap-only `JSON_EXISTS` | 1,099 | 0 | 0 | 646x faster, allocation-free |
| One-time subcolumn materialization | 1,341,533 | 653,815 | 10,511 | promotion/build cost |

The source fixture occupied `35,768` bytes while the retained complex-path
representation occupied `14,780` bytes, a 2.42x reduction. `JSON_QUERY` still
allocates its decoded result because its public result is a Go map/slice; the
optimization avoids decoding unrelated document fields and avoids retaining
those fields between reads. The promotion cost should therefore be amortized
over repeated reads.

Raw output from `make benchmark-chu20`:

```text
BenchmarkCHU20JSONSubcolumnMaterialize-32              963  1340554 ns/op  26.68 MB/s  653826 B/op  10511 allocs/op
BenchmarkCHU20JSONSubcolumnMaterialize-32              928  1402143 ns/op  25.51 MB/s  653801 B/op  10511 allocs/op
BenchmarkCHU20JSONSubcolumnMaterialize-32              924  1331925 ns/op  26.85 MB/s  653791 B/op  10511 allocs/op
BenchmarkCHU20JSONSubcolumnMaterialize-32              940  1341533 ns/op  26.66 MB/s  653815 B/op  10511 allocs/op
BenchmarkCHU20JSONSubcolumnMaterialize-32              915  1345475 ns/op  26.58 MB/s  653777 B/op  10511 allocs/op
BenchmarkCHU20JSONQueryRowFallback-32                 1592   707919 ns/op   0.36 MB/s   35768 source_bytes/op  489430 B/op  7927 allocs/op
BenchmarkCHU20JSONQueryRowFallback-32                 1579   778758 ns/op   0.33 MB/s   35768 source_bytes/op  489439 B/op  7927 allocs/op
BenchmarkCHU20JSONQueryRowFallback-32                 1810   750916 ns/op   0.34 MB/s   35768 source_bytes/op  489429 B/op  7927 allocs/op
BenchmarkCHU20JSONQueryRowFallback-32                 1641   746441 ns/op   0.34 MB/s   35768 source_bytes/op  489432 B/op  7927 allocs/op
BenchmarkCHU20JSONQueryRowFallback-32                 1710   731300 ns/op   0.35 MB/s   35768 source_bytes/op  489429 B/op  7927 allocs/op
BenchmarkCHU20JSONQueryColumnarLateMaterialized-32    2312   439348 ns/op   0.58 MB/s   14780 column_bytes/op  266101 B/op  5367 allocs/op
BenchmarkCHU20JSONQueryColumnarLateMaterialized-32    2851   468031 ns/op   0.55 MB/s   14780 column_bytes/op  266101 B/op  5367 allocs/op
BenchmarkCHU20JSONQueryColumnarLateMaterialized-32    2598   460720 ns/op   0.56 MB/s   14780 column_bytes/op  266100 B/op  5367 allocs/op
BenchmarkCHU20JSONQueryColumnarLateMaterialized-32    2992   437706 ns/op   0.58 MB/s   14780 column_bytes/op  266100 B/op  5367 allocs/op
BenchmarkCHU20JSONQueryColumnarLateMaterialized-32    2827   452795 ns/op   0.57 MB/s   14780 column_bytes/op  266101 B/op  5367 allocs/op
BenchmarkCHU20JSONExistsRowFallback-32                1636   729382 ns/op   0.35 MB/s  489407 B/op  7927 allocs/op
BenchmarkCHU20JSONExistsRowFallback-32                1620   710116 ns/op   0.36 MB/s  489407 B/op  7927 allocs/op
BenchmarkCHU20JSONExistsRowFallback-32                1720   689993 ns/op   0.37 MB/s  489407 B/op  7927 allocs/op
BenchmarkCHU20JSONExistsRowFallback-32                1873   691355 ns/op   0.37 MB/s  489407 B/op  7927 allocs/op
BenchmarkCHU20JSONExistsRowFallback-32                1892   785903 ns/op   0.33 MB/s  489408 B/op  7927 allocs/op
BenchmarkCHU20JSONExistsColumnar-32                1000000     1146 ns/op  223.45 MB/s  0 B/op  0 allocs/op
BenchmarkCHU20JSONExistsColumnar-32                1000000     1045 ns/op  245.03 MB/s  0 B/op  0 allocs/op
BenchmarkCHU20JSONExistsColumnar-32                1041888     1099 ns/op  233.04 MB/s  0 B/op  0 allocs/op
BenchmarkCHU20JSONExistsColumnar-32                1045014     1134 ns/op  225.75 MB/s  0 B/op  0 allocs/op
BenchmarkCHU20JSONExistsColumnar-32                1089188     1095 ns/op  233.70 MB/s  0 B/op  0 allocs/op
```

The focused correctness target is `make test-chu20`. Race and vet targets for
the feature should be run before delivery alongside the repository's broader
verification targets.
