# Reusable Top-K Results

`hatDataStructure.TopK.EntriesInto` is an opt-in result-buffer API for callers
that repeatedly read a bounded top-k summary. It writes the same deterministic
ordering as `Entries` into caller-owned storage and reuses the backing array
when capacity is sufficient.

```go
scratch := make([]hatDataStructure.TopKEntry[string], 0, top.Capacity())
for poll := 0; poll < 100; poll++ {
	entries := top.EntriesInto(scratch[:0])
	consume(entries)
}
```

The method temporarily orders the private counter slice by output order, fills
the destination, and rebuilds the internal min-heap before returning. This is
safe because `TopK` is already documented as not safe for concurrent use; the
public counts, errors, tie ordering, and subsequent updates remain unchanged.
`Entries()` remains the compatibility API and still returns an independent
copy.

`make benchmark-top-k-entries` used ten `-benchmem` samples with 128 retained
counters and compared repeated `Entries()` calls with a preallocated
`EntriesInto` destination:

| Path | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| `Entries()` | 10,519 | 10,360 | 5 | baseline |
| reusable `EntriesInto` | 3,923 | 0 | 0 | 2.68x faster |

The benefit requires the caller to retain and reuse the destination slice.
First-use growth still allocates when its capacity is too small.
