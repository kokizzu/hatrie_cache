# M-U38 Persisted Immutable Data Parts

`hatStorage.ImmutablePartManifest` adds the missing publication boundary for
Materialize-style immutable data parts. It records a complete, checksummed set
of remote objects and lets a caller publish the set atomically after the
objects are durable.

## Lifecycle

1. Write and checksum each object with the existing remote-part or multipart
   upload APIs.
2. Build a manifest with one `ImmutableDataPart` per object. Each part has a
   stable `PartID`, partition, generation, optional opaque key bounds, row
   count, and `RemotePartMetadata`.
3. Call `ImmutablePartCatalog.Publish`. The catalog validates every reference,
   writes a compact binary manifest to a temporary file, fsyncs it, renames it
   atomically, and syncs the containing directory.
4. Use `ImmutablePartPublication.Added` for newly reachable objects and
   `Retired` for parts no longer referenced by the current manifest. A retired
   part is not deleted automatically.
5. After the configured retention window, pass the current manifest's
   `RemotePartReferences()` to `PlanRemotePartGarbageCollection` together with
   the object-store listing. Review and apply that existing deletion plan
   separately.

The publication order is therefore upload/checksum, publish manifest, wait for
retention, then review GC. A crash before publication leaves unreferenced
objects that GC can retain until an operator explicitly plans their removal;
a crash after publication leaves a complete current manifest.

## Storage Format

Binary is the default persistence format. It uses the `HIP1` header, bounded
length-prefixed fields, deterministic partition/part ordering, and a SHA-256
trailer. `LoadImmutablePartManifest` rejects truncation, tampering, invalid
references, path traversal, and unsupported versions.

JSON remains an interoperability fallback at the call site:

```go
payload, err := json.Marshal(manifest) // fallback/export format
if err != nil {
	return err
}
var decoded hatStorage.ImmutablePartManifest
if err := json.Unmarshal(payload, &decoded); err != nil {
	return err
}
decoded, err = decoded.Normalize()
```

The JSON fallback is larger and should not replace the binary catalog file for
normal storage.

## Safety Guarantees

- Generation must strictly increase on publication.
- Reusing a `PartID` with different immutable metadata is rejected.
- Publication never uploads or deletes objects; those side effects remain
  explicit and caller-owned.
- Existing catalog files and staging files are regular files with mode `0600`;
  symlink catalog paths are rejected.
- Returned manifests, references, and transition slices are independent
  copies.
- The catalog serializes writers within one process. Cross-process locking and
  object-store authorization remain deployment responsibilities.

## Benchmark

Workload: 512 normalized parts, five `go test -benchmem` samples on an AMD
Ryzen 9 5950X. Relative values use JSON as `1.00x`.

| Operation | Binary | JSON fallback | Binary result |
| --- | ---: | ---: | ---: |
| Marshal latency | ~140 us/op | ~202 us/op | 1.45x faster |
| Marshal heap | 90,112 B/op | ~160,142 B/op | 1.78x lower |
| Encoded size | 59,461 B | 148,589 B | 2.50x smaller |
| Decode latency | ~226 us/op | ~1,409 us/op | 6.23x faster |
| Decode heap | 135,193 B/op | 224,327 B/op | 1.66x lower |
| Decode allocations | 3,586 | 3,604 | 1.01x lower |

The binary format wins on durable size, marshal/decode CPU, and heap for this
workload. JSON remains useful for interoperability when a human-readable
payload is more important than storage and transfer efficiency.

Focused verification is available through the repository's `make` workflow:

```text
make test-mu38
make benchmark-mu38
```
