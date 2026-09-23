# CH-G45: `EXPLAIN ESTIMATE`

Status: implemented.

`EXPLAIN ESTIMATE` is a ClickHouse-inspired spelling for the existing
read-only heuristic cost plan. It returns the same `estimated_rows`,
`estimated_cost`, and `estimated_memory_bytes` columns as `EXPLAIN COST` and
does not execute the query or read a source unless `ANALYZE` is also requested.

```sql
EXPLAIN ESTIMATE
FROM VALUES (1), (2), (3) AS values(id)
WHERE id > 0
SELECT id
```

`EXPLAIN COST` remains supported and produces an identical plan and result.
The new keyword does not prevent `estimate` from being used as a column or
alias identifier. Existing regular `EXPLAIN` and `EXPLAIN ANALYZE` behavior is
unchanged.

## Measurements

Linux/amd64, AMD Ryzen 9 5950X, Go benchmark `-benchtime=100ms -count=5`:

| Operation | CPU range | Heap | Allocs | Result |
| --- | ---: | ---: | ---: | --- |
| Existing `EXPLAIN COST` baseline | 10.174–10.918 us/op | 11,870 B/op | 66 | Existing behavior |
| `EXPLAIN ESTIMATE` | 10.445–11.379 us/op | 11,811–11,813 B/op | 64 | Same cost plan |
| Direct `EXPLAIN COST` control, same fixture | 10.296–11.143 us/op | 11,811–11,813 B/op | 64 | No measurable alias cost |

The alias adds no cost to ordinary query execution because it is recognized
only in the EXPLAIN prefix. Verification targets are provided by the
repository Makefile: `test-chg45`, `test-chg45-related`, `race-chg45`,
`vet-chg45`, and `benchmark-chg45`.
