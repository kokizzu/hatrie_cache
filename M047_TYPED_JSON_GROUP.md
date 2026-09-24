# M047 Typed JSON `GROUP BY`

M047 adds an opt-in columnar fast path for the narrow query shape:

```sql
SELECT JSON_VALUE(doc, '$.user.id') AS id, COUNT(*) AS n
FROM CACHE('items')
GROUP BY JSON_VALUE(doc, '$.user.id')
```

The resolver supplies a validated `ColumnarJSONSubcolumn` for the scalar path.
The executor then groups directly on compact `int64`, `float64`, string, or
boolean values and keeps only one count per key. Missing paths and JSON null
are grouped as SQL `NULL`, matching the existing `JSON_VALUE` result behavior.

Supported safeguards include row limits, cancellation/deadlines, maximum group
keys, maximum rows per key, group memory budgets, result byte budgets, and
ordinary `WHERE` predicates supported by the typed JSON scan path.

The optimization is deliberately narrow. `ORDER BY`, `HAVING`, joins, unions,
grouping sets, windows, complex expressions, non-`COUNT(*)` aggregates, arrays,
objects, and unavailable or malformed subcolumns use the existing row executor.
No caller changes are required: a resolver that does not implement the
optional typed-subcolumn contract continues to work unchanged.

## Measurement

Command:

```text
make benchmark-m047
```

The benchmark executes a 10,000-row, 128-group query three times per variant
on an AMD Ryzen 9 5950X Linux `amd64` host. The legacy and typed resolver
variants use the same query and fallback rows; the typed variant additionally
serves the compact path column.

| Variant | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Legacy row JSON control | 19,464,939 | 19,194,917 | 172,147 | 1.00x |
| Typed JSON grouped path | 786,329 | 70,020 | 439 | 24.8x faster, 274x lower measured bytes, 392x fewer allocations |

Raw output:

```text
BenchmarkM047TypedJSONSubcolumnGroupByCount/legacy-row-json-32         61  19464939 ns/op 19195038 B/op 172147 allocs/op
BenchmarkM047TypedJSONSubcolumnGroupByCount/legacy-row-json-32         64  19448287 ns/op 19194917 B/op 172147 allocs/op
BenchmarkM047TypedJSONSubcolumnGroupByCount/legacy-row-json-32         55  19488699 ns/op 19194721 B/op 172146 allocs/op
BenchmarkM047TypedJSONSubcolumnGroupByCount/typed-subcolumn-32       1500    786329 ns/op    70020 B/op    439 allocs/op
BenchmarkM047TypedJSONSubcolumnGroupByCount/typed-subcolumn-32       1588    788974 ns/op    70020 B/op    439 allocs/op
BenchmarkM047TypedJSONSubcolumnGroupByCount/typed-subcolumn-32       1477    776238 ns/op    70019 B/op    439 allocs/op
```

Correctness coverage is in
`hat/hatSql/m047_typed_json_group_test.go`, including scalar kinds,
NULL/missing keys, filtering, limits, and fallback.
