# TT-018 Page-index Residency Policy

`hatDataStructure.PageIndexResidency[K, V]` is an opt-in bounded LRU for
immutable page-index objects. It addresses the Tarantool/Vinyl-style need to
keep hot page indexes resident without allowing an unbounded index cache to
consume process memory.

## Use

```go
residency, err := hatDataStructure.NewPageIndexResidency[string, []byte](
	hatDataStructure.PageIndexResidencyOptions{
		MaxBytes:   64 << 20,
		MaxEntries: 1024,
	},
)
if err != nil {
	// A zero byte budget disables the optional policy.
}

residency.Put(partKey, pageIndexBytes, estimatedBytes)
if cached, ok := residency.Get(partKey); ok {
	// Use the immutable resident page index.
	_ = cached
}
stats := residency.Stats()
```

`Put` does not copy `V`; callers must treat admitted page indexes as immutable.
`sizeBytes` is the caller's byte charge for the retained object. An entry
larger than `MaxBytes` is rejected without evicting an existing entry. When
the byte or entry limit is reached, the least recently used entries are
evicted first.

`Get` updates recency, returns a hit or miss, and allocates zero heap objects
on the measured path. `Delete`, `Clear`, `Len`, and `Stats` support explicit
maintenance and read-only telemetry. `Stats` reports current bytes and
entries plus hits, misses, admissions, replacements, evictions, and
rejections.

## Scope and tradeoffs

The policy is independent of `SparsePrimaryIndex`, storage engines, SQL
planning, persistence, and replication. Existing defaults are unchanged; a
storage caller must explicitly create it and decide when to populate or
invalidate it. The byte budget is the caller-declared page-index charge, so a
hard process-memory budget should include allocator and metadata overhead in
that charge.

This policy deliberately does not replace a raw map for unrestricted hot
lookups. LRU recency updates require synchronization and make hits slower than
a plain map. The benefit is bounded residency and observable eviction rather
than lower lookup latency.

See the paired measurements in [BENCHMARK.md](BENCHMARK.md#tt-018-page-index-residency-policy).
