# SQL Named Settings Collections

`hatSql` provides versioned named settings profiles for query and storage
callers. A profile is a bounded map of string values, published as an
immutable atomic snapshot. This follows ClickHouse named collections while
keeping parsing and application of domain-specific settings with the caller.

## Usage

```go
registry, err := hatSql.NewSQLNamedSettingsRegistry(hatSql.SQLNamedSettingsRegistryOptions{})
if err != nil {
    return err
}

profile, err := registry.Put("analytics", map[string]string{
    "max_rows": "100000",
    "timeout":  "5s",
    "format":   "columnar",
})
if err != nil {
    return err
}

resolved, err := registry.Resolve("analytics", map[string]string{
    "timeout": "10s",
})
if err != nil {
    return err
}
applyQueryOrStorageSettings(resolved.Values)

if timeout, ok := registry.LookupValue("analytics", "timeout"); ok {
    applyTimeout(timeout)
}

_, err = registry.PutIfRevision("analytics", profile.Revision, map[string]string{
    "max_rows": "200000",
    "timeout":  "5s",
    "format":   "columnar",
})
```

`Resolve` captures one consistent base revision and applies caller-owned
overrides without changing the published collection. `PutIfRevision` and
`DeleteIfRevision` provide compare-and-swap updates; a stale writer receives
`ErrSQLNamedSettingsConflict` instead of silently overwriting a newer profile.

## Bounds And Defaults

- The registry has no goroutine and adds no default SQL execution work.
- The zero value is usable.
- Defaults are 256 collections, 128 settings per collection, and 16 KiB per
  value.
- Configured limits are validated and bounded to prevent accidental excessive
  memory reservation.
- Collection names and setting keys are bounded to 256 bytes.
- `Lookup` and `Snapshot` return copies, so callers cannot mutate published
  state. `LookupValue` reads one immutable string without cloning the profile.
- `Stats` reports revision and counts without exposing configuration values.

## Benchmark

The benchmark compares the registry with a raw two-entry Go map. Updates and
full profile reads intentionally pay for immutable-copy publication or caller
isolation; the single-value read path stays allocation-free.

See [BENCHMARK.md](BENCHMARK.md#ch-050-named-settings-collections) for every
raw sample and the median table.
