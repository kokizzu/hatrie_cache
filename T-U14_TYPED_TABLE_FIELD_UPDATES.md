# T-U14 Typed-table field updates

T-U14 adopts a narrow Tarantool-style tuple field update path for
`hatSql.TypedTable`. It updates an existing row without resolving the full
row into SQL maps and without requiring callers to construct a complete row.

```go
change, err := table.Update("event-0", []hatSql.TypedTableUpdate{
	{Column: "count", Kind: hatSql.TypedTableUpdateAdd, Value: hatSql.TypedInt64(1)},
	{Column: "label", Kind: hatSql.TypedTableUpdateSet, Value: hatSql.TypedString("hot")},
})
```

## Supported operations

- `TypedTableUpdateSet` replaces one field. `TypedNull()` sets SQL `NULL`.
- `TypedTableUpdateAdd` supports non-null `Int64` and `Float64` values only.
- A request contains at most 64 operations and cannot mention one column twice.
- Every field, type, null rule, and arithmetic result is validated before any
  column is changed.
- A successful request emits one `TypedTableChange` with `Operation == "UPDATE"`.

Invalid requests, missing keys, arithmetic overflow, memory-budget failures,
and generated-column tables leave the row and change sequence unchanged.
Tables with generated columns return `ErrTypedTableUpdateGenerated`; use
`Upsert` when generated values need to be recomputed. Inserts and full-row
replacement continue to use `Upsert`.

## Measurement

Five `go test -benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X. The
control reads one row through `ResolveSQLSource` and writes it back through
`Upsert`. The direct path increments the same `Int64` field through `Update`.
Lower is better.

| Path | Median ns/op | Median B/op | Median allocs/op | CPU vs pre-change | Allocation bytes vs pre-change | Allocations vs pre-change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before: read plus full `Upsert` | 1,098 | 1,374 | 10 | 1.00x | 1.00x | 1.00x |
| After: direct field `ADD` | 647.2 | 916 | 4 | 1.70x faster | 1.50x less | 2.50x fewer |
| After control rerun: read plus full `Upsert` | 1,029 | 1,386 | 10 | 1.07x faster | 0.99x | 1.00x |

Raw samples (`ns/op / B/op / allocs/op`):

```text
before control: 1098/1374/10, 1070/1380/10, 1019/1374/9, 1100/1374/9, 1127/1374/10
after direct:    677.1/919/4, 689.0/925/4, 638.9/877/4, 647.2/916/4, 646.3/916/4
after control:  1002/1341/10, 1051/1389/10, 1050/1369/10, 1029/1386/10, 997.1/1386/10
```

The direct path still returns immutable before/after changefeed values and
performs the same derived-cache invalidation, TTL refresh, and memory-budget
admission checks as a normal update. The benchmark measures allocation volume,
not total process RSS.

Reproduce with:

```sh
make benchmark-t-u14-before-script
make benchmark-t-u14-typed-table-update
```

Correctness and race coverage:

```sh
make test-t-u14-typed-table-update
make race-t-u14-typed-table-update
make vet-t-u14-typed-table-update
```
