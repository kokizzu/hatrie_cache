# Token Bloom Filter

`TokenBloomFilter` is a public, compact prefilter for word-oriented text
search. It is available from both `hatrie_cache` and
`hatrie_cache/hat/hatDataStructure`.

```go
filter, err := hatriecache.NewTokenBloomFilter(4096, 0.01)
if err != nil {
	return err
}
filter.AddText("Fast low-memory storage for cafe users")

// A true result permits an exact search; it does not replace one.
if filter.ContainsAllTokens("FAST storage") {
	// Recheck the original text or row here.
}
```

Tokens are contiguous Unicode letters or digits. Punctuation and whitespace
separate tokens, and simple Unicode lower-casing makes `FAST` match `fast`.
`AddToken` and `ContainsToken` accept one token only; empty or
punctuation-containing values are ignored. `ContainsAllTokens` is useful for
an AND-style query and returns true for a query with no tokens. The
`ContainsAnyTokens` helper is useful for an OR-style query and returns false
for a query with no tokens.

Bloom filters can return false positives. They must be used only to skip a
candidate that is definitely absent; every possible match needs the exact
word-search predicate. The zero value is valid but unconfigured. Backing words
are allocated lazily on the first successful add, and the snapshot is the
existing compact `BloomFilterSnapshot` format.

## Measurements

Measurements used Go benchmarks with five samples, `-benchtime=250ms`, on an
AMD Ryzen 9 5950X. The initial implementation and the final implementation
have identical semantics and both allocate zero heap bytes in these paths.
The final pass avoids revalidating token ranges that the text scanner has
already proved valid.

| Operation | Initial median | Final median | Result | Heap / allocs |
| --- | ---: | ---: | ---: | --- |
| `ContainsAllTokens`, three tokens | 156.7 ns | 111.8 ns | 1.40x faster | 0 B/op, 0 allocs/op |
| `ContainsAnyTokens`, two tokens | 94.51 ns | 80.47 ns | 1.17x faster | 0 B/op, 0 allocs/op |
| `AddText`, five tokens | 277.8 ns | 277.4 ns | neutral within run noise | 0 B/op, 0 allocs/op |

The filter stores only its bitset and fixed header; it never stores the input
strings or a token map. For example, a `1<<20`-bit filter has a 131,072-byte
bitset after its first add, while an empty configured filter retains no bitset.
The compact representation trades exact membership and deletions for bounded
memory and conservative filtering.

Run the benchmark with:

```text
make benchmark-token-bloom
```
