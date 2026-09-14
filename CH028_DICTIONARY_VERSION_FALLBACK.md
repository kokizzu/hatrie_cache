# CH-028 Dictionary Version And Fallback Semantics

`hat/hatDictionary` supports optional source versions and explicit fallback
sources. Existing `Source` implementations remain valid and are treated as
unversioned; sources that need snapshot consistency can implement
`VersionedSource`.

## Versioned Reads

```go
source := hatDictionary.VersionedSourceFunc(func(ctx context.Context, keys []string) (map[string]string, string, error) {
	values, version, err := loadDimensionSnapshot(ctx, keys)
	return values, version, err
})
dictionary, err := hatDictionary.New(source, hatDictionary.Options{})
if err != nil {
	return err
}

refresh, err := dictionary.RefreshAtVersion(ctx, keys, "catalog-2026-09-15")
result, err := dictionary.LookupAtVersion(ctx, "country:SG", refresh.Version)
```

`RefreshAtVersion` publishes no values when the source returns a different
version and returns `ErrVersionMismatch`. `LookupAtVersion` accepts a cached
entry only when its stored version matches the requested version; if the
version is absent or different, it refreshes through the source. A caller that
needs to discover a new source version must explicitly refresh, while a
cached expected-version read remains stable and cheap.

## Explicit Fallback

```go
dictionary, err := hatDictionary.New(primarySource, hatDictionary.Options{
	Fallback:        localSnapshotSource,
	FallbackOnMiss:  true,
	FallbackOnError: true,
})
```

Fallback is disabled unless the corresponding option is enabled. On a primary
miss, only missing keys are sent to the fallback. On a primary error, the
fallback receives the complete requested batch. Fallback values are marked by
`LookupResult.Fallback`, retain the fallback source version, and are counted in
`RefreshResult.Fallback`; primary values are never silently overwritten.

When an expected version is supplied, fallback values must report that same
version. This prevents a stale or incompatible fallback snapshot from being
mixed into a version-pinned read. A source error with no successful fallback
still leaves prior cache entries untouched and can use the CH-027
`StaleIfError` policy when explicitly enabled.

## Tradeoffs

Version metadata adds a string reference per retained entry but no per-hit
allocation. Version checks make snapshot consistency deterministic; they do
not poll the source on every lookup. Fallback improves availability only when
explicitly configured and can return older data, so the result exposes its
fallback status. The source remains responsible for authenticating and
authorizing both primary and fallback loads.

Benchmark details and raw samples are in
[BENCHMARK.md#ch-028-dictionary-version-and-fallback](BENCHMARK.md#ch-028-dictionary-version-and-fallback).
