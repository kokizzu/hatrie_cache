# Native Extension Boundary

`hat/hatExtension` is an importable contract for optional native or external
extensions. It is deliberately a boundary, not a shared-object loader:

- `Manifest` validates a stable ABI, artifact checksum, platform, entry point,
  and capability list.
- `Loader` is implemented by the application that owns the loading policy. It
  may use cgo, a subprocess, WASI, or another sandbox.
- `Registry` applies optimistic version fencing and generation numbers.
- `Resolve` is the zero-allocation active-extension lookup path.
- `Metadata` and `Snapshot` return owned copies, so callers cannot mutate the
  registry's manifest state.
- `Invoke` is never called by the registry; the application decides when and
  how to execute an extension.

No native code is loaded, executed, or unloaded by this package. This keeps the
default SQL and cache paths unchanged and prevents an accidental `.so` loader
from becoming a new default attack surface.

## Example

```go
package main

import (
	"context"

	"hatrie_cache/hat/hatExtension"
)

func install(registry *hatExtension.Registry, loader hatExtension.Loader) error {
	manifest := hatExtension.Manifest{
		Name:           "geo",
		Version:        "1.2.3",
		ABI:            hatExtension.NativeExtensionABIV1,
		EntryPoint:     "hatrie_extension_init_v1",
		Platform:       "linux/amd64",
		ChecksumSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Capabilities:   []hatExtension.Capability{hatExtension.CapabilitySQLFunction},
	}

	_, err := registry.Load(context.Background(), loader, manifest, "")
	return err
}
```

For a replacement, pass the active version returned by `Metadata` as
`expectedVersion`. A stale replacement or unregister fails with
`ErrVersionConflict`; a same-version replacement fails with
`ErrVersionUnchanged`.

The loader must verify the artifact represented by `ChecksumSHA256` before it
returns an adapter. `Registry.Load` then validates that the adapter reports the
same normalized manifest, preventing a loader from silently returning a
different name, version, ABI, platform, entry point, or capability set.

## Security Boundary

The package does not claim that native code is safe. The caller-owned loader
must choose the trust model, isolate untrusted code when needed, enforce file
permissions and signature policy, and decide how an old adapter is drained
before unregistering it. The registry only provides identity and lifecycle
fencing.

The existing `hat/hatSql.PluginRegistry` remains an in-process Go extension
registry. It is unchanged and does not automatically use this package.

## Verification

```text
make t103-native-test-red
make t103-native-test
make t103-native-race
make t103-native-vet
make t103-native-benchmark-baseline
make t103-native-benchmark-after
```
