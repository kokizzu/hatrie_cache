# Mergeable Approximate Top-K State

`hatDataStructure.TopK[K]` is a bounded Space-Saving summary for comparable
keys. It retains at most `capacity` counters, so retained state is `O(k)` even
when the input has high cardinality.

```go
partialA, err := hatDataStructure.NewTopK[string](100)
if err != nil {
    panic(err)
}
partialA.Add("queued")
partialA.AddN("running", 3)

partialB, _ := hatDataStructure.NewTopK[string](100)
partialB.AddN("queued", 4)
if err := partialA.Merge(partialB); err != nil {
    panic(err)
}

for _, item := range partialA.Entries() {
    // item.Count-item.Error <= true frequency <= item.Count.
    fmt.Println(item.Key, item.Count, item.Error)
}
```

`AddN` is useful when a partial aggregate already has a multiplicity. `Merge`
combines two bounded summaries without replaying every input row; the receiver
capacity controls the result. `Snapshot` and `NewTopKFromSnapshot` provide a
validated portable state boundary. The type is not synchronized for concurrent
updates, so callers should assign one owner or add external synchronization.

## Measurement

Five isolated `-benchmem` samples processed 100,000 deterministic string values
with 20,000 possible keys and `k=100` on an AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Exact map plus full sort | 11,370,700 | 1,127,740 | 68 | baseline |
| Bounded `TopK` state | 26,976,785 | 21,872 | 12 | 2.37x CPU, 51.6x lower bytes |

This is intentionally an opt-in bounded-memory/distributed-partial primitive,
not a replacement for an exact low-cardinality map or for the existing SQL
`APPROX_TOP_K` evaluator. The CPU cost buys a hard memory bound and mergeable
partial state; callers that do not need those properties should keep the exact
or existing SQL path.
