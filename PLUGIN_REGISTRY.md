# Versioned Plugin Registry

`hat/hatSql.PluginRegistry` provides an opt-in in-process module selection
boundary for `hatSql.Plugin` implementations. It supports atomic initial
load, expected-version replacement, version-checked unload, metadata snapshots,
and lock-free-from-the-caller lookup. Existing plugin interfaces and function
registration are unchanged.

```go
registry := hatSql.NewPluginRegistry()
if _, err := registry.Load(pluginV1, ""); err != nil {
	return err
}

// Replace only if the caller still owns the version it observed.
if _, err := registry.Load(pluginV2, "1.0.0"); err != nil {
	return err
}

active, ok := registry.Resolve("geo")
metadata, ok := registry.Metadata("geo")
```

An empty expected version is allowed only for an initial load. Replacements
and unloads require the active version, so a stale deploy cannot overwrite or
remove a newer module. Every successful install receives a monotonically
increasing generation. A replacement with the same version is rejected.

The registry swaps interface values atomically under a read/write lock; it does
not load native shared objects, execute untrusted code, or unload Go packages.
The application remains responsible for constructing and validating plugin
implementations, draining work before replacement, and choosing compatible
versions. Native FFI remains a separate security-sensitive design boundary.

## Verification

```text
make test-plugin-registry-red-clean
make benchmark-plugin-registry-clean
```
