# CH-020: Zero-Copy Part Sharing

Status: partially adopted.

`hatMerkle.SharedPartRegistry` provides a bounded, importable registry for
verified immutable-part references. A replica can acquire a lease for a shared
location and read the existing bytes without receiving or copying another
payload. The registry owns metadata and lease state only; storage, transport,
authorization, and deletion remain caller-owned.

## Lifecycle

1. `Register` normalizes the share ID, source replica, part name, and location,
   clones the manifest metadata, and calls the caller-provided verifier before
   publication.
2. Registering the same immutable descriptor is idempotent. Reusing a share ID
   with different metadata is rejected.
3. `Acquire` returns a detached metadata reference and a lease ID. Existing
   readers keep the share valid even after retirement.
4. `Retire` prevents new readers. `Remove` succeeds only after retirement and
   after every lease is released.

The registry never opens, reads, copies, or deletes `Entry.Location`. A caller
must ensure the location is a valid shared filesystem/object reference and must
verify the manifest and access authorization before registration.

## Defaults And Limits

- The feature is opt-in; creating a registry does not alter `PartCatalog` or
  replication behavior.
- Default maximum registered descriptors: `1,024`.
- Default maximum concurrent leases: `4,096`.
- Both bounds are capped at `1<<20`.
- Share IDs, source replica IDs, and locations are limited to 256 bytes and
  reject NUL bytes.

## Benchmark

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X. The
baseline clones a 1 MiB payload. The shared path registers its descriptor
outside the timer and measures repeated acquire/release of an already verified
reference.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 MiB payload clone | 150,489; 124,192; 138,216; 153,889; 194,424 | 150,489 | 1,048,585 | 1 | 1.00x |
| Shared reference lease | 162.2; 157.8; 162.7; 168.3; 162.6 | 162.6 | 0 | 0 | 925.5x faster |

The shared path avoids one 1 MiB heap copy per reuse and therefore avoids that
payload's local transfer bandwidth. The benchmark does not claim a network
speedup when replicas do not share storage; a caller must choose a valid shared
location and still perform its own manifest/authentication checks.

## Verification

The `Makefile` contains focused test, package, race, benchmark, vet, and
verification targets under `ch020-zero-copy-part-sharing`. Tests cover metadata
detachment, verifier failure, idempotent registration, conflicts, lease
capacity, retirement, removal fencing, duplicate release, and descriptor
validation.
