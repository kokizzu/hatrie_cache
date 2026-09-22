# T030 On-Replace Changefeed Hooks

`hatDataStructure.MemtxTable` supports an optional typed `OnReplace` callback
through `NewMemtxTableWithHooks`. It receives the old and new tuple images,
stable ID, existence flag, and operation kind after a successful `Insert` or
`Upsert` has updated the table.

```go
table, err := hatDataStructure.NewMemtxTableWithHooks[int](
    hatDataStructure.MemtxTableOptions{Capacity: 1024},
    hatDataStructure.MemtxTableHooks[int]{
        OnReplace: func(event hatDataStructure.MemtxReplaceEvent[int]) {
            changefeed.Publish(event.ID, event.Old, event.New)
        },
    },
)
```

Events are emitted synchronously under the table write lock, in mutation order,
and only after the row is stored. A rejected `BeforeReplace` write emits no
event. The callback must not call back into the table or block for an
unbounded period. When both hooks are configured, `OnReplace` sees the value
returned by `BeforeReplace`.

## Measurement

AMD Ryzen 9 5950X, Linux amd64, `-benchmem -benchtime=3s -count=3`, one hot
row. Both paths measured `0 B/op` and `0 allocs/op`.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| T230 baseline, no `OnReplace` | 12.34 | 0 | 0 |
| T230, enabled `OnReplace` | 12.98 | 0 | 0 |

The opt-in hook costs about 5.2% CPU in this longer same-process comparison;
the default path is unchanged when the hook is nil.
