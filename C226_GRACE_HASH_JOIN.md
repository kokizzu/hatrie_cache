# C226: Grace-Hash Join Spilling

## Status

C226 is verified in the existing SQL executor. This progress commit adds
focused verification targets and documentation; the production grace-hash
join path was already implemented.

For a direct two-source inner equality join with bounded join and spill
budgets, the executor:

- streams both inputs into 64 hash partitions;
- keeps at most one right-side partition chunk in memory;
- probes the left partition against that chunk;
- bounds joined output with spill runs and merges by source ordinals;
- optionally skips disjoint partitions using Bloom filters; and
- removes all temporary files on success, cancellation, and quota failure.

Unsupported joins retain the existing executor or return the explicit policy
diagnostic. The default policy and ordinary in-memory join behavior are
unchanged.

## Verification

The focused tests cover duplicate keys, deterministic output order, streamed
input use, plan reporting, and spill-budget cleanup:

```sh
make test-c226-grace-hash-join
make race-c226-grace-hash-join
```

## Measurement

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X. The
workload is a six-row equality join from the existing C229 policy benchmark.

| Mode | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Auto/default in-memory | 21,949 | 18,344 | 116 | 1.00x |
| Explicit reject budget | 25,611 | 19,064 | 140 | 1.17x |
| Bounded spill | 5,777,393 | 858,570 | 5,196 | 263.2x |

The spill path is intentionally not a speed optimization for small joins. Its
value is bounded memory and forward progress when the in-memory hash table
would exceed `MaxJoinBytes`; operators should use it when that safety property
outweighs temporary-file CPU, heap, and allocation cost.

Run the measurement with:

```sh
make benchmark-c226-grace-hash-join
```
