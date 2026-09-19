# CH-U24: Typed-Table Memory Budget

`TypedTableSchema.MemoryBudget` adds an opt-in logical resident-data budget to
the importable typed-table API:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    Columns: []hatSql.TypedTableColumn{
        {Name: "region", Kind: hatSql.TypedTableString},
        {Name: "value", Kind: hatSql.TypedTableInt64},
    },
    MemoryBudget: hatSql.TypedTableMemoryBudgetOptions{
        MaxBytes: 64 << 20,
    },
})
```

`MaxBytes: 0` is the default and preserves the existing behavior. Negative
limits are rejected. `Upsert` and `AppendColumnar` validate the complete
mutation before changing table state; an over-budget mutation returns
`ErrTypedTableMemoryBudgetExceeded`. `MemoryUsage` exposes the configured
limit, current estimate, and remaining admission space.

The estimate is deliberately deterministic and cheap: a fixed row allowance,
key bytes, one validity byte per column, and scalar payload bytes. It does not
pretend to cover Go map capacity, indexes, SQL working memory, allocator
fragmentation, or other tables. Applications that need a hard process-level
RSS limit still need an external cgroup or process policy.

With patch parts enabled, a delete remains charged until
`CompactPatchParts` physically removes the row. This matches actual retained
storage and avoids admitting new data based on bytes that have not been
released. The budget is per `TypedTable`; it is not a cluster-wide or query
budget.

## Tradeoff

The feature is disabled by default. On Linux/amd64 with an AMD Ryzen 9 5950X,
five `-benchmem` samples of repeated existing-row `Upsert` measured:

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Budget disabled | 267.5 | 192 | 4 |
| Budget enabled | 276.4 | 192 | 4 |

The enabled guard was about 3.3% slower in this small write benchmark and did
not add allocations. The guard protects against unbounded logical row growth;
it is not a replacement for measuring the full heap of a workload.

See the raw samples in [BENCHMARK.md](BENCHMARK.md#ch-u24-typed-table-memory-budget).
