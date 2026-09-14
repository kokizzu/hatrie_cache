# CH-027 External Dictionary Cache

`hat/hatDictionary` provides a bounded cache for externally loaded dimension
values. It is a ClickHouse-inspired dictionary primitive, but it is a public
Go package and does not add a SQL planner dependency.

## Usage

```go
source := hatDictionary.SourceFunc(func(ctx context.Context, keys []string) (map[string]string, error) {
	return loadDimensionRows(ctx, keys)
})
dictionary, err := hatDictionary.New(source, hatDictionary.Options{})
if err != nil {
	return err
}

result, err := dictionary.Lookup(ctx, "country:SG")
if errors.Is(err, hatDictionary.ErrStale) {
	// result.Value is usable, but the source refresh failed.
}
if result.Found {
	use(result.Value)
}
```

For efficient warm-up, callers can load a batch explicitly:

```go
refresh, err := dictionary.Refresh(ctx, []string{"country:SG", "country:JP"})
```

`Refresh` deduplicates keys and rejects batches above `MaxRefreshKeys` before
calling the source. Missing keys are not retained. A source failure leaves
existing values unchanged.

## Defaults And Bounds

`Options{}` uses 4,096 entries, 16 MiB of logical key-plus-value bytes, a
five-minute TTL, and a 256-key refresh limit. `StaleIfError` is disabled by
default. Enable it only when availability is more important than strict
freshness; a stale result is returned with `Stale=true` and an error matching
both `ErrStale` and the source error.

The cache uses an approximate-LRU eviction order. Entry access ticks and
activity counters are atomic, so fresh hits use a read lock and do not allocate
or take a write lock. The cache starts no goroutine; refresh scheduling remains
under the caller's control, which avoids lifecycle leaks in embedded services.

Values are strings deliberately: they are immutable from the cache's
perspective and avoid a defensive byte-slice copy on every hit. The byte bound
accounts for the logical key and value lengths; map and entry metadata add a
small implementation overhead.

Keys are trimmed and empty or NUL-containing keys are rejected. Source calls
receive the caller context, and implementations should enforce their own
authentication, authorization, timeout, and secret-handling policy.

## Tradeoff And Measurement

The benchmark compares a one-key in-memory source load with a warm dictionary
hit. It does not pretend to measure network latency; remote source latency
makes cache hits more valuable, while the cache's retained bytes are the
explicit memory cost.

Results and raw samples are in
[BENCHMARK.md#ch-027-external-dictionary-cache](BENCHMARK.md#ch-027-external-dictionary-cache).
