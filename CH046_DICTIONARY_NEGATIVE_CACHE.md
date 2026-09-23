# CH046 Dictionary Negative Cache

The dictionary can optionally retain source misses for a bounded time. This
prevents repeated lookups for a permanently absent key from refreshing the
source on every call.

## Configuration

Negative caching is disabled by default. Enable it explicitly:

```go
dictionary, err := hatDictionary.New(source, hatDictionary.Options{
	NegativeTTL:        30 * time.Second,
	MaxNegativeEntries: 1024,
})
```

`NegativeTTL: 0` keeps the existing behavior. `MaxNegativeEntries: 0` uses
the default bound of 1,024 entries. The existing `MaxEntries` and `MaxBytes`
limits still apply to the combined positive and negative cache. A key larger
than `MaxBytes` is returned as a miss but is not retained as a negative entry.

## Behavior

- A source miss is returned as `LookupResult{Found: false}` and, when enabled,
  retained until `NegativeTTL` expires.
- A fresh negative entry avoids a source call and increments `Stats.NegativeHits`.
- A positive source result replaces a negative entry, including a positive empty
  string, which remains distinguishable as `Found: true`.
- Versioned lookups retain the source version on negative entries. A request for
  another version refreshes instead of reusing the old miss.
- Negative entries are never returned by `StaleIfError`; only positive stale
  values can be served as stale fallbacks.
- Negative entries participate in the existing LRU ordering and have their own
  `Stats.NegativeEntries` count.

## Measurement

The benchmark repeats the same missing-key lookup after a one-time warm-up.
Five samples were collected on the repository's AMD Ryzen 9 5950X host:

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Improvement |
| --- | --- | ---: | ---: | ---: | --- |
| Existing source miss | 202.5; 201.0; 205.1; 202.6; 199.1 | 202.5 | 32 | 2 | baseline |
| Bounded negative-cache hit | 102.5; 105.1; 106.6; 100.5; 96.09 | 102.5 | 0 | 0 | 1.98x faster; 32 B and 2 allocations eliminated |

The feature adds one retained cache entry per cached miss, including the key's
logical bytes and normal map/object overhead. `NegativeTTL`, `MaxNegativeEntries`,
`MaxEntries`, and `MaxBytes` bound that cost. With the default-off setting,
existing callers do not retain negative entries and follow the previous path.

Run the focused checks with:

```text
make m239-ch-g46-test
make m239-ch-g46-benchmark
```
