# M-U33 Storage Compaction Retention Gate

`hatStorage.CompactionRequest` now accepts an optional `CompactionRetentionGate`.
When configured, the compaction controller waits for
`WaitUntilSafe(ctx, frontierID, boundary)` before invoking the storage callback.
`hatPipeline.FrontierRetentionRegistry` satisfies the interface without making
`hatStorage` import the pipeline package.

The gate is opt-in. Requests with a nil gate retain the existing scheduler and
controller behavior. A gate requires a non-empty frontier name; supplying
frontier metadata without a gate is rejected. If the wait is canceled or fails,
the existing retry state is preserved and the storage callback is not invoked.

## Measured Cost

The focused controller benchmark creates, submits, and runs one job per
iteration on Linux/amd64 (AMD Ryzen 9 5950X, Go benchmark `-count=5`):

| Path | Time range | Memory | Allocs |
| --- | ---: | ---: | ---: |
| Clean base, legacy request | 1,087-1,122 ns/op | 1,392 B/op | 15 |
| Feature branch, legacy request | 967-1,028 ns/op | 1,360 B/op | 15 |
| Feature branch, immediate retention gate | 1,029-1,080 ns/op | 1,408 B/op | 15 |

The legacy result is within normal benchmark variance and has no added
allocation count. The retention path pays approximately 48 additional bytes
for the opt-in gate metadata and callback capture; the default path does not
pay that cost.
