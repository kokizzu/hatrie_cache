# CH-U31 Multipart Remote Upload

`hatStorage.UploadRemotePartMultipart` is an opt-in, sequential coordinator for
uploading one immutable remote part in resumable multipart pieces. It keeps the
storage-specific network, authentication, and object-store implementation
behind `RemoteMultipartUploadStore`.

## Usage

```go
result, err := hatStorage.UploadRemotePartMultipart(
    ctx,
    store,
    "s3://bucket/parts/part-0001",
    "metadata/part-0001.json",
    payload,
    hatStorage.RemoteMultipartUploadOptions{
        PartSize: 8 << 20,
        State:    loadCheckpointOrZero(),
        Persist: func(state hatStorage.RemoteMultipartUploadState) error {
            return saveCheckpoint(state)
        },
    },
)
if err != nil {
    // result.State contains the resumable progress after a part failure.
    return err
}
reference := result.Reference
_ = reference
```

The first call initiates an upload. A later call supplies the persisted state
with the same object URI, metadata path, payload size, whole-object checksum,
and part size. Completed parts with a non-empty ETag are skipped; missing parts
are uploaded and persisted one at a time. The persistence callback runs after
initiation and after every successful part, so a crash loses at most the last
successful part whose checkpoint was not durable.

The default part size is 8 MiB and the coordinator rejects configurations that
would require more than 10,000 parts. A caller may choose a larger or smaller
part size when the remote store has different limits.

## Integrity and recovery

- The object and every part use canonical lowercase `sha256:<hex>` checksums.
- Resume state is validated before any network call, including offsets, sizes,
  duplicate part numbers, checksums, ETags, object identity, and payload size.
- The coordinator does not automatically abort on an error. This preserves
  resumability and lets the caller decide whether a transient failure should be
  retried or the remote upload should be abandoned.
- Use `hatStorage.AbortRemoteMultipartUpload` for an explicit cleanup decision.
- The store must consume each `data` slice before `UploadMultipartPart`
  returns; the coordinator slices the caller payload and does not copy it.
- Remote object-store authorization, server-side checksum enforcement, retry
  policy, orphan-upload discovery, and retention remain store/operator policy.

The object URI and local metadata path are passed through the existing remote
part reference validation. The API does not accept arbitrary local metadata
paths or silently rewrite a resumed upload to a different object.

## Measured cost

The benchmark uses a 4 MiB payload, a no-op store, five `-benchmem` samples,
Linux/amd64, and an AMD Ryzen 9 5950X. Raw samples are shown in execution
order; medians are the middle sorted sample.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| One whole-payload SHA-256 | 1,959,165; 1,983,738; 1,973,534; 1,940,585; 1,963,822 | 1,963,822 | 9 | 0 | 1.00x |
| Fair two-checksum baseline | 3,938,290; 3,941,050; 4,005,544; 4,037,843; 3,842,402 | 3,941,050 | 18 | 0 | 2.01x |
| Multipart coordinator, one part | 4,096,522; 4,033,336; 4,007,616; 3,925,734; 3,956,409 | 4,007,616 | 866 | 12 | 2.04x vs one checksum; 1.02x vs fair baseline |
| Multipart coordinator, four parts | 3,913,537; 4,051,921; 4,022,129; 3,960,868; 3,954,873 | 3,960,868 | 2,131 | 21 | 2.02x vs one checksum; 1.01x vs fair baseline |

The coordinator is close to the expected whole-object-plus-part checksum work.
Its additional memory is control-plane metadata, not a second 4 MiB payload:
about 867 B and 12 allocations for one part, or 2,131 B and 21 allocations
for four parts. The no-op store means network bandwidth and remote latency are
not measured here; production measurements should include the store adapter,
TLS, retries, and the object-store service.
