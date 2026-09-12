# M204: Bounded Journal Subscriptions

`CommandJournalSubscribeOptions.UpToSequence` provides a finite, exclusive upper bound for a journal subscription. It is useful for consumers that need to drain a known journal interval and then close without racing a live tail.

```go
subscription, err := journal.Subscribe(ctx, hatCache.CommandJournalSubscribeOptions{
	AfterSequence: 10,
	UpToSequence:  20,
})
if err != nil {
	return err
}

for record := range subscription.Records() {
	process(record)
}
if err := subscription.Err(); err != nil {
	return err
}
```

The example delivers sequences `11` through `19`, then closes `Records()` with a nil subscription error. Sequence `20` and later are never delivered. `UpToSequence: 0` preserves the existing unbounded behavior. `AfterSequence` and `UpToSequence` must describe a non-empty ordered range unless `SkipReplay` is enabled; `SkipReplay` continues to ignore `AfterSequence` and starts from the current tail.

The bound works with key prefixes, exact spaces, and coalescing. Bounded replay still enforces `ReplayLimit`; the implementation can ignore records after the upper bound when deciding whether the requested interval fits within the limit. A subscription whose bound is already at or behind its starting cursor closes without emitting records.

The journal's sequence is the project's monotonic logical timestamp. This feature does not add historical state or permit reads from an earlier snapshot; it only bounds delivery of the durable journal stream.

## Compatibility and cost

The default remains unbounded, so existing callers and wire formats are unchanged. The bound adds one sequence comparison to subscription notification and delivery paths. The focused notification benchmark measured no allocations:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Pre-change unbounded baseline | 84.16 | 0 | 0 |
| Final unbounded path | 80.01 | 0 | 0 |
| Final bounded path | 80.93 | 0 | 0 |

The bounded versus unbounded final median is `1.01x` (`+1.1%`) on an AMD Ryzen 9 5950X, Linux amd64, Go, five samples, `-benchtime=1s -benchmem`. The result has no allocation or memory increase; the small CPU difference is workload- and machine-dependent.

Raw final `ns/op` samples: unbounded `79.93, 81.07, 80.01, 79.71, 80.96`; bounded `80.93, 79.72, 74.57, 83.56, 88.39`.

## Verification

```text
make verify-m204
make benchmark-m204
```
