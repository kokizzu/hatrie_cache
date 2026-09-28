# Mergeable Approximate Sketches

`HyperLogLog.Merge` and `QuantileSketch.Merge` combine compatible partial
states without replaying the source rows.

```go
left, _ := hatDataStructure.NewHyperLogLog(14)
right, _ := hatDataStructure.NewHyperLogLog(14)
left.AddJSONString(`"east"`)
right.AddJSONString(`"west"`)
if err := left.Merge(right); err != nil {
    panic(err)
}

low, _ := hatDataStructure.NewQuantileSketch(0.01)
high, _ := hatDataStructure.NewQuantileSketch(0.01)
low.AddValidBatch(firstValues)
high.AddValidBatch(secondValues)
if err := low.Merge(high); err != nil {
    panic(err)
}
```

HyperLogLog merging takes the register-wise maximum and saturating-adds the
observation count. Quantile merging combines sorted rank summaries, carries
the source rank uncertainty into the merged tuples, compresses under the
combined allowance, and validates the resulting snapshot. Precision or epsilon
mismatches are rejected. Both types remain caller-owned and are not safe for
concurrent mutation.

## Measurement

Five isolated `-benchmem` samples on an AMD Ryzen 9 5950X compared two already
partitioned inputs with the cost of replaying every raw value into one sketch.

| State | Replay baseline | Merge | CPU result | Heap / allocs |
| --- | ---: | ---: | --- | --- |
| HyperLogLog, 8,192 values | 102,810 ns/op | 2,956 ns/op | 34.8x faster | 1,024 / 1,024 B/op, 1 / 1 alloc |
| QuantileSketch, 4,096 values | 148,731 ns/op | 1,020 ns/op | 146x faster | 1,512 / 2,624 B/op, 6 / 6 allocs |

The quantile merge allocates a combined summary copy, trading a bounded `1.74x`
heap increase for avoiding raw-row replay. Existing SQL approximate aggregate
evaluation remains unchanged; callers opt into partial-state merging explicitly.
