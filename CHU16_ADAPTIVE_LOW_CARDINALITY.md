# CH-U16 Adaptive Low-Cardinality Admission

`hatDataStructure.NewAdaptiveLowCardinalityStringBuilder` is an opt-in
dictionary admission policy for workloads whose cardinality is not known in
advance. It keeps the existing sorted dictionary encoding while the input is
low-cardinality, then switches once to an exact raw string column when the
distinct-value limit or sampled distinct ratio says dictionary encoding is no
longer appropriate.

```go
builder := hatDataStructure.NewAdaptiveLowCardinalityStringBuilder(
	hatDataStructure.AdaptiveLowCardinalityStringBuilderOptions{
		MaxDistinctValues:     4096,
		MaxDistinctRatio:      0.5,
		MinRowsBeforeFallback: 64,
	},
)
for _, value := range values {
	if err := builder.Append(value); err != nil {
		panic(err)
	}
}
column, err := builder.Build()
if err != nil {
	panic(err)
}
if column.UsesDictionary() {
	// CodeAt/LookupCode are compact dictionary operations.
} else {
	// ValueAt/Contains remain exact on the raw fallback.
}
```

The existing `LowCardinalityStringBuilder` and its default limit are
unchanged. This new builder is not automatically selected by SQL or storage.
For a raw fallback, `Contains` is a linear scan and `Cardinality` builds a
temporary scratch set; no second lookup map is retained. That keeps retained
memory bounded, but callers with known high cardinality should choose a plain
string column directly instead of paying adaptive admission overhead.

## Measurement

Five `-count=5` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X. Each build ingests 10,000 prepared strings.

| Workload | Existing control | Adaptive | Relative result |
| --- | ---: | ---: | --- |
| 256 distinct values | 190,443 ns/op, 82,232 B/op, 13 allocs/op | 217,098 ns/op, 82,520 B/op, 14 allocs/op | 1.14x slower, +288 B, +1 alloc |
| 4,096 distinct values | 1,108,272 ns/op, 525,776 B/op, 27 allocs/op | 1,127,762 ns/op, 526,064 B/op, 28 allocs/op | 1.02x slower, +288 B, +1 alloc |
| 10,000 distinct values | Plain raw slice: 55,487 ns/op, 163,840 B/op, 1 alloc/op | 166,800 ns/op, 668,792 B/op, 17 allocs/op | 3.01x slower, 4.08x bytes, 17x allocs |

The existing dictionary builder rejects the 10,000-distinct workload at its
distinct limit, so the raw slice is a capability control rather than an
equivalent dictionary baseline. The adaptive path is useful when rejection is
unacceptable and the caller cannot know cardinality ahead of time; it is not a
replacement for a deliberately chosen raw representation.

Raw samples and the reproducible command are in [BENCHMARK.md](BENCHMARK.md).
Run:

```text
make benchmark-chu16-adaptive-low-cardinality
```
