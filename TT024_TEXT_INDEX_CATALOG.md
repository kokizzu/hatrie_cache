# TT-024 External Text Index Catalog

`TextIndexCatalog` is an opt-in boundary for storing the existing positional
text-index `HTI1` frame in an application-owned durable catalog. It adds no
default I/O, background workers, global state, or automatic restore behavior.

## API

```go
type TextIndexCatalog interface {
    PutTextIndex(ctx context.Context, sourceKey, field string, frame []byte) error
    GetTextIndex(ctx context.Context, sourceKey, field string) ([]byte, error)
}
```

Typical startup code is:

```go
source.BuildTextIndex("body")
if err := source.PersistTextIndex(ctx, catalog, "docs-eu", "body"); err != nil {
    return err
}

restored := hatSchema.NewMaterializedSource(columns)
// Load rows into restored before applying the source-digest-checked frame.
if err := restored.RestoreTextIndexFromCatalog(ctx, catalog, "docs-eu", "body"); err != nil {
    return err
}
```

`PersistTextIndex` calls `MarshalTextIndex`, then passes the bounded,
deterministic `HTI1` frame to `PutTextIndex`. `RestoreTextIndexFromCatalog`
calls `GetTextIndex`, then delegates validation and publication to
`RestoreTextIndex`. The frame has a CRC32C checksum and a source-field digest;
rows must already represent the same source snapshot or restoration fails.

Source keys and field names are trimmed and must be non-empty. A canceled
context is rejected before work and checked again around the catalog call. A
nil context is treated as `context.Background()` for compatibility, but
callers should pass a real request or startup context.

## Catalog responsibilities

The catalog implementation owns durability, retention, replication, locking,
namespacing, and authorization. It should copy a frame if it retains the byte
slice, return an independent or immutable frame from `GetTextIndex`, and apply
its own size and quota limits. An application can back the interface with a
file, embedded database, Redis, Tarantool, or another service without making
the core source depend on that system.

The `HTI1` checksum detects corruption; it is not an authentication mechanism.
Untrusted catalogs still need access control and, when required, encryption or
an authenticated envelope. Restoring a frame never executes code, but callers
should treat catalog errors and source-key construction as security-sensitive.

This API persists one field per call. Cross-field phrase planning, mixed
boolean planner integration, and automatic catalog discovery remain outside
this feature.

## Cost

The default path is unchanged because a source does not retain a catalog and
does not call it unless the caller explicitly invokes one of the two methods.
On the in-memory benchmark catalog, the hook added about 8.1% median persist
CPU and 5.4% median restore CPU relative to direct HTI1 marshal/restore. The
restore comparison added one allocation and about 3.46 KB/op because the fake
catalog defensively copied the 3,266-byte frame; a real catalog has different
I/O and buffering costs.

See [BENCHMARK.md](BENCHMARK.md#tt-024-external-text-index-catalog-hook) for
raw output and the direct controls.
