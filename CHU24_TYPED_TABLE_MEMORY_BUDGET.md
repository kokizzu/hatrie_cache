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

`MemoryUsage.ReservedBytes` reports explicit caller-owned reservations. Use
`ReserveMemory` for bounded index, arrangement, or query working memory:

```go
reservation, err := table.ReserveMemory(8 << 20)
if err != nil {
    return err
}
if reservation != nil {
    defer reservation.Release()
}
```

`Release` is idempotent. With `MaxBytes: 0`, `ReserveMemory` returns
`(nil, nil)` without taking the table lock or allocating, so the default path
is unchanged. The reservation remains a logical admission contract; it does
not automatically measure Go map capacity, allocator fragmentation, or RSS.

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
| Budget disabled | 228.9 | 192 | 4 |
| Budget enabled | 238.3 | 192 | 4 |

The enabled guard was about 4.1% slower in this small write benchmark and did
not add allocations. The guard protects against unbounded logical row growth;
it is not a replacement for measuring the full heap of a workload.

## Working-Memory Reservation Tradeoff

The following five-sample run was measured on the same linux/amd64 host after
adding the reservation counter. The existing upsert paths retained their
allocation profile. The small before/after CPU differences are benchmark
noise, not a claimed optimization.

| Path | Before median ns/op | After median ns/op | After B/op | After allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Budget-disabled upsert | 228.9 | 224.0 | 192 | 4 | 0.98x |
| Budget-enabled upsert | 238.3 | 228.8 | 192 | 4 | 0.96x |
| Disabled `ReserveMemory(64)` | n/a | 8.231 | 0 | 0 | new no-op path |
| Enabled reserve + release | n/a | 40.60 | 24 | 1 | explicit opt-in cost |

Run `make codex-chu24-memory-reservation-benchmark` to reproduce the raw
samples. The lease is intended for bounded setup or query scopes, not for a
per-row hot loop.

See the raw samples in [BENCHMARK.md](BENCHMARK.md#ch-u24-typed-table-memory-budget).
