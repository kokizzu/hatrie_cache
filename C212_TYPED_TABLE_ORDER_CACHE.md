# C212 Typed-Table Sorted Order Cache

This feature adopts a narrow Materialize-style ordered arrangement for the
typed-table columnar SQL path. It is deliberately opt-in and bounded.

## Configuration

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    Columns: []hatSql.TypedTableColumn{
        {Name: "id", Kind: hatSql.TypedTableInt64},
        {Name: "score", Kind: hatSql.TypedTableInt64},
    },
    ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
        Enabled:          true,
        SortedOrderCache: true,
        MaxBytes:         4 << 20,
        MinReads:         2,
    },
})
```

`SortedOrderCache` defaults to `false`. `ColumnarCache.Enabled` must also be
true. The existing columnar cache admits the requested field layout according
to `MinReads`; the order vector is then admitted after eight compatible order
requests. The order vector consumes four bytes per active row and counts toward
the same `MaxBytes` limit.

## Execution

For a supported direct typed-table query:

```sql
FROM CACHE('events') AS item
SELECT item.id, item.score
ORDER BY item.score ASC
LIMIT 50
```

the SQL executor first tries the existing borrowed columnar layout and then
the admitted ascending ordinal projection. It walks only the needed ordered
ordinals to produce the page. Descending queries reuse the ascending vector in
reverse through the existing SQL projection path.

The implementation currently supports one string or numeric order field with
ordinary binary comparison and stable row-ordinal ties. Nullable, NaN,
boolean, expression, explicit-null-order, grouped, joined, and otherwise
unsupported shapes retain the established fallback. Composite and range-aware
ordered arrangements are separate follow-up work.

## Correctness

The focused tests cover:

- admission only after repeated compatible requests;
- ascending ordinal order and SQL result order;
- stable source order for equal values through the ordinal tie break;
- cache invalidation after `Upsert`;
- disabled-by-default behavior;
- exact four-byte-per-row order accounting and the `MaxBytes` bound.

Every successful `Upsert` or `Delete` clears the derived columnar layouts and
their order observations before the mutation is visible. A concurrent build
is published only if its source sequence still matches the cached layout.

## Measurement

On an AMD Ryzen 9 5950X, a 20,000-row table and a 50-row page measured:

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Columnar top-N scan before | 1,321,778 | 169,791 | 17,629 |
| Admitted sorted ordinal projection after | 15,659 | 21,128 | 122 |

The result is 84.4x faster, with 8.0x lower transient bytes and 144.5x fewer
allocations. The retained vector for this fixture is 80,000 bytes plus cache
metadata. The one-time admission sort and the retained vector are the explicit
costs; leaving the feature disabled has no new read or write cost.
