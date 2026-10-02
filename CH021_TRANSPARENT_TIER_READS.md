# CH-021 Transparent Storage-Tier Reads

This feature adds an opt-in `hatStorage.StorageTierReader` for immutable parts
that may be stored on more than one tier. It keeps the existing
`StorageTierPolicy`, move planner, and direct read APIs unchanged.

## Behavior

- The reader tries the declared current tier first.
- It retries on the age-selected tier only when the read callback returns
  `ErrStorageTierPartNotFound`.
- Other errors are returned without a second read.
- If the current tier is already the age-selected tier, a not-found result is
  returned without retrying the same tier.
- The reader performs no filesystem or object-storage I/O itself. The callback
  owns local reads, remote reads, authentication, retries, and byte ownership.
- The returned bytes are passed through without a copy; callers must apply
  their normal immutable-buffer ownership rules.
- The feature is opt-in. Existing storage-tier behavior and defaults do not
  change.

## Measurement

The benchmark uses the same two-tier policy and a callback that returns a
stable payload without allocating. Each result is checked during the loop.
The direct baseline calls the current-tier selector directly.

| Path | Median ns/op | B/op | allocs/op | Relative to direct baseline |
| --- | ---: | ---: | ---: | ---: |
| Direct current-tier selection baseline | 11.61 | 0 | 0 | 1.00x |
| Transparent reader, current-tier hit | 27.29 | 0 | 0 | 2.35x |
| Transparent reader, not-found fallback | 42.94 | 0 | 0 | 3.70x |

Raw five-run samples:

```text
direct:   11.61, 11.89, 11.67, 11.31, 10.97 ns/op
current:  27.29, 27.67, 27.67, 25.96, 25.84 ns/op
fallback: 42.63, 42.94, 42.29, 44.14, 44.84 ns/op
```

The overhead is the cost of validation, callback dispatch, and fallback
decision-making. It is zero-allocation and opt-in; callers that already know
the exact tier can continue using the lower-cost direct selector or their own
read path.

Run the focused checks with:

```text
make format-round18-ch021-tier-reader
make test-round18-ch021-tier-reader
make race-round18-ch021-tier-reader
make vet-round18-ch021-tier-reader
make benchmark-round18-ch021-tier-reader
```
