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

The implementation supports one or more string or numeric order fields with
ordinary binary comparison and stable row-ordinal ties. Uniform ascending
composite orders use `BorrowSQLColumnarSourceOrderFields`; mixed-direction
orders use `BorrowSQLColumnarSourceOrderBy` and retain a separate vector for
each direction pattern. Nullable, NaN, boolean, expression,
explicit-null-order, grouped, joined, and otherwise unsupported shapes retain
the established fallback. Range-aware ordered arrangements and arbitrary
signed-update maintenance remain separate follow-up work.

## Correctness

The focused tests cover:

- admission only after repeated compatible requests;
- ascending ordinal order and SQL result order;
- composite ascending order, mixed directions, and deterministic ties;
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
| Columnar top-N scan before | 1,432,985 | 170,414 | 17,634 |
| Admitted sorted ordinal projection after | 21,613 | 21,640 | 122 |

The result is 66.3x faster, with 7.9x lower transient bytes and 144.5x fewer
allocations. The retained vector for this fixture is 80,000 bytes plus cache
metadata. The one-time admission sort and the retained vector are the explicit
costs; leaving the feature disabled has no new read or write cost.

Raw samples from `make benchmark-mz038`:

```text
Before: 1452410, 1415420, 1475182, 1422617, 1432985 ns/op; 170400-170514 B/op; 17634-17636 allocs/op
After:  20847, 21231, 21613, 22013, 21663 ns/op; 21640 B/op; 122 allocs/op
```

### Composite ordered projection

Command:

```text
make benchmark-mz038
```

This benchmark uses the same 20,000-row table and a 50-row page, with
`ORDER BY score ASC, id DESC`. The before samples are the cache-disabled
control collected before composite support; the after samples are the warmed
steady-state admitted projection.

| Path | Raw ns/op (5 runs) | Median ns/op | B/op | Allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | --- |
| Composite top-N control | 3,299,175; 3,275,129; 3,382,226; 3,213,544; 3,314,648 | 3,299,175 | 993,161 | 60,287 | 1.00x |
| Composite admitted projection | 20,119; 19,867; 20,386; 20,038; 20,626 | 20,119 | 22,120 | 126 | 164.0x faster; 44.9x lower bytes; 478.5x fewer allocations |

The retained composite vectors use four bytes per active row per distinct
direction pattern. The one-time admission sort, vector retention, and write
invalidation are bounded costs; `SortedOrderCache` remains disabled by default
and all unsupported shapes use the existing fallback.
