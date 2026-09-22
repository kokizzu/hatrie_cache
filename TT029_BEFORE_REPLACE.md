# T029 Before-Replace Hooks

`hatDataStructure.MemtxTable` now supports an optional typed `BeforeReplace`
hook through `NewMemtxTableWithHooks`. The hook receives the ID, old value,
proposed value, existence flag, and operation kind for each `Insert` or
`Upsert` write. It can return a normalized value or an error that rejects the
write before the table changes.

```go
table, err := hatDataStructure.NewMemtxTableWithHooks[int](
    hatDataStructure.MemtxTableOptions{Capacity: 1024},
    hatDataStructure.MemtxTableHooks[int]{
        BeforeReplace: func(event hatDataStructure.MemtxReplaceEvent[int]) (int, error) {
            if event.Exists && event.New < event.Old {
                return 0, errors.New("value cannot decrease")
            }
            return event.New, nil
        },
    },
)
```

The callback runs under the table write lock. It must be deterministic and must
not call back into the table. A rejected write leaves the old row, slot usage,
and capacity unchanged. The default constructor has no hook and keeps the
existing allocation-free update path.

## Measurement

AMD Ryzen 9 5950X, Go benchmark with `-benchmem -count=5`, one hot row:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before T029, no hook | 11.43 | 0 | 0 |
| After T029, no hook | 10.75 | 0 | 0 |
| After T029, enabled hook | 12.92 | 0 | 0 |

The default path showed no regression in this five-sample run; the measured
5.9% lower median is within normal short-benchmark variance and is not treated
as a performance claim. The opt-in validation/normalization hook costs roughly
20.2% versus the new default path, with no allocation or retained-memory cost.
