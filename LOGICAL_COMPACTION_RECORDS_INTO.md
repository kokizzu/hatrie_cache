# Logical Compaction `RecordsInto`

`LogicalCompaction.RecordsInto` returns retained differential records in
ascending timestamp order while reusing a caller-owned destination slice.
`Records()` remains the ownership-safe convenience API and now delegates to
the same implementation.

```go
records := make([]DifferentialRecord[int], 0, compaction.Len())
for range snapshots {
    records = compaction.RecordsInto(records)
    consume(records)
}
```

The method clears and reuses the destination, grows it only when capacity is
insufficient, and copies record values rather than exposing the internal map.
Relative order for records sharing a timestamp remains unspecified, matching
the existing contract.

## Measurement

Workload: 4,096 retained records with distinct timestamps, measured with
`go test -benchmem -count=10` on an AMD Ryzen 9 5950X.

| Operation | Median ns/op | B/op | Allocs/op | Relative to old `Records()` |
| --- | ---: | ---: | ---: | ---: |
| Old `Records()` with stable reflective sort | 2,055,789 | 98,400 | 4 | 1.00x |
| Current `Records()` with typed sort | 313,854 | 98,304 | 1 | 6.55x faster |
| Reused `RecordsInto()` with typed sort | 281,138 | 0 | 0 | 7.31x faster |

The typed `slices.SortFunc` is valid because equal-timestamp order is not part
of the API contract. The existing `Records()` result remains an independent
copy; `RecordsInto` is opt-in for callers that can retain a buffer.
