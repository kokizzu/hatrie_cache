# T031 After-Replace Audit Hooks

`hatDataStructure.MemtxTable` supports an optional typed `AfterReplace`
callback through `NewMemtxTableWithHooks`. The callback receives a table-local
transaction identity separately from the old/new `MemtxReplaceEvent`, after a
successful `Insert` or `Upsert` has committed.

```go
table, err := hatDataStructure.NewMemtxTableWithHooks[int](
    hatDataStructure.MemtxTableOptions{Capacity: 1024},
    hatDataStructure.MemtxTableHooks[int]{
        AfterReplace: func(transactionID uint64, event hatDataStructure.MemtxReplaceEvent[int]) {
            audit.Log(transactionID, event.ID, event.Old, event.New)
        },
    },
)
```

Transaction identities start at `1` per table and advance only for successful
mutations that reach `AfterReplace`; rejected writes do not consume an ID or
emit an audit event. The callback runs under the table write lock after
`OnReplace`, so it must not call back into the table or block indefinitely.
The default path and tables without `AfterReplace` do not allocate or advance
the audit counter.

## Measurement

AMD Ryzen 9 5950X, Linux amd64, `-benchmem -benchtime=3s -count=3`, one hot
row. Both paths measured `0 B/op` and `0 allocs/op`.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| T231 baseline, no `AfterReplace` | 12.29 | 0 | 0 |
| T231, enabled `AfterReplace` | 15.40 | 0 | 0 |

The opt-in audit hook costs about 25.3% CPU in this small mutation benchmark;
the default path is unchanged. A pointer-event alternative was measured and
rejected because it added `48 B/op` and `1 alloc/op`.
