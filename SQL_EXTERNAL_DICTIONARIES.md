# SQL External Dictionaries

`hatSql` provides an opt-in, in-process lookup registry for small reference
datasets that are refreshed independently from the main SQL data. This follows
the ClickHouse external-dictionary idea while keeping the lookup path local.

## Lifecycle

Create a dictionary with a loader and publish it in a registry:

```go
dictionary, err := hatSql.NewSQLExternalDictionary(hatSql.SQLExternalDictionaryOptions{
    Name: "countries",
    Load: func(ctx context.Context) (map[string]interface{}, error) {
        return loadCountries(ctx)
    },
    RefreshInterval: 5 * time.Minute,
    MaxStale:        30 * time.Minute,
})
if err != nil {
    return err
}

if err := dictionary.Start(ctx); err != nil {
    return err
}

registry := hatSql.NewSQLExternalDictionaryRegistry()
if err := registry.Register(dictionary); err != nil {
    return err
}
defer dictionary.Close()
```

`Start` performs the first load synchronously, then refreshes on the configured
interval. `Refresh` can be called directly for an on-demand update. A refresh
builds a complete immutable snapshot and publishes it atomically, so readers
always use either the previous complete snapshot or the new complete snapshot.

The last successful snapshot remains available when a later load fails. When
`MaxStale` is positive, lookups fail with `ErrSQLExternalDictionaryExpired`
after that bound is exceeded. A dictionary with no successful load returns
`ErrSQLExternalDictionaryNotReady`.

## SQL Functions

The registry resolves these case-insensitive functions:

| Function | Arguments | Result |
| --- | --- | --- |
| `DICT_GET` | dictionary name, key | Stored value, or `NULL` when the key is absent |
| `DICT_GET_OR_DEFAULT` | dictionary name, key, default | Stored value, or the supplied default when the key is absent |
| `DICT_HAS` | dictionary name, key | Boolean indicating whether the key exists |

Dictionary names and keys are strings. Values are copied at publish and lookup
time for supported scalar values and byte slices, preventing callers from
mutating a published snapshot. Unsupported value types are rejected by the
loader boundary.

`SQLExternalDictionaryRegistry` can be placed in a resolver chain. A resolver
that does not recognize a function returns `ErrSQLFunctionNotHandled`, allowing
the next resolver to handle normal SQL functions.

## Defaults And Safety

- Refreshing is opt-in; constructing a dictionary does not start a goroutine.
- `RefreshInterval` must be positive when using `Start`.
- `MaxStale` defaults to unlimited staleness when set to zero.
- Per-lookup counters are disabled by default. Set `CollectStats: true` only
  when the counters are needed for diagnostics or operational metrics.
- Failed refreshes do not replace a good snapshot.
- `Close` stops background refresh and rejects later refresh and lookup calls.

## Performance

The direct dictionary API is intended for reference lookups, not as a drop-in
replacement for a raw Go map. On the local benchmark workload, the default
zero-allocation lookup measured a median of 49.98 ns/op versus 7.10 ns/op for
the raw map baseline. It used 0 B/op and 0 allocations/op. The extra time buys
atomic snapshot publication, lifecycle checks, stale-data policy, and value
copying.

The one-call SQL-function path measured 114.7 ns/op, 16 B/op, and 1 alloc/op.
Refreshing a 256-entry dictionary measured 22.45 us/op, 37,600 B/op, and 265
allocations/op. These measurements are workload-specific; run
`make benchmark-ch049-external-dictionary` before changing the hot path.

See the raw samples and comparison table in
`BENCHMARK.md#ch-049-refreshable-external-dictionaries`.
