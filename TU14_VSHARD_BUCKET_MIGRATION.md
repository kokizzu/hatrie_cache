# T-U14 VShard Bucket Migration

This feature adopts the VShard-style idea of a deterministic bucket map plus
an explicit, resumable ownership-transfer state machine. It is importable from
`hat/hatTopology` and is disabled unless an embedding service calls it.

## What It Provides

`PlanBucketMigrations(current, target)` compares two validated sharded
topologies and returns one deterministic `BucketMigrationPlan` per changed
bucket. A plan records:

- the bucket and whether the operation is a primary-shard move or a backup-set
  change;
- source and target primary/replica ownership;
- topology fingerprints and fencing tokens;
- a stable plan ID suitable for retries and durable caller-side records.

`BucketMigrationCoordinator` bounds and tracks the lifecycle:

`planned -> copying -> ready -> cutover -> completed`

An interrupted operation can move to `aborted` and later `resume` from its
retained counters. Copy progress rejects uint64 overflow. Cutover requires an
exact target bucket, ownership snapshot, fingerprint, and fencing token.

## Caller Responsibilities

The primitive intentionally does not open a network connection, copy records,
replay a WAL, write a topology file, or publish cluster membership. A service
should:

1. Generate plans from the current and proposed topology.
2. Persist or distribute the plan ID and source/target ownership contract.
3. Copy the bucket and apply changes until source and target journal positions
   are exactly equal.
4. Call `MarkCutoverReady`, publish the already-validated target ownership
   through the service's fencing/consensus path, then call `CompleteCutover`.
5. Abort on failure and resume only after the caller has verified that the
   source and target snapshots are still valid.

The coordinator is deliberately not a substitute for quorum consensus,
replica repair, durable transfer logs, or backup scheduling.

## Safety Defaults And Limits

- No migration runs automatically and existing routing is unchanged.
- A zero `MaxMigrations` uses `DefaultBucketMigrationMaxMigrations` (`256`).
- A configured limit is capped at `4096` to bound in-memory coordinator state.
- Plan IDs are required to be non-empty, trimmed, and at most 256 bytes.
- A move must change shard ID; a backup migration must retain shard ID.
- Target journal sequence must equal the source sequence before cutover.
- Repeating the exact plan ID is idempotent; reusing it for another plan fails.

## Measurement

Command:

```text
make benchmark-tu14-vshard
```

The benchmark uses five runs on an AMD Ryzen 9 5950X. The baseline is the
previous caller-side operation: normalize both snapshots and count changed
bucket owners. The planner additionally builds immutable ownership contracts,
fingerprints, and deterministic plan IDs, so it is not expected to beat that
bare count loop.

| Operation | Median ns/op | B/op | allocs/op | Relative to baseline |
| --- | ---: | ---: | ---: | ---: |
| Before: manual bucket diff | ~2,284 | 2,368 | 32 | 1.00x |
| After: `PlanBucketMigrations` | ~4,622 | 3,173 | 90 | 2.02x CPU, 1.34x bytes, 2.81x allocations |
| After: full coordinator lifecycle | ~2,953 | 2,016 | 51 | control-plane only |

Representative raw samples from the same run (`ns/op`, `B/op`, `allocs/op`):

```text
Before: 2284 2368 32
Before: 2290 2368 32
Before: 2264 2368 32
Before: 2291 2368 32
Before: 2243 2368 32
After planner: 4700 3173 90
After planner: 4627 3173 90
After planner: 4584 3173 90
After planner: 4612 3173 90
After planner: 4622 3173 90
After lifecycle: 2953 2016 51
After lifecycle: 2961 2016 51
After lifecycle: 2950 2016 51
After lifecycle: 3004 2016 51
After lifecycle: 2925 2016 51
```

An initial implementation measured about 9.2 microseconds, 6.0 KB, and 174
allocations per planner call because each ownership lookup normalized the
topology and recomputed its fingerprint. The implementation now reuses the
normalized snapshots and precomputed fingerprints. The remaining overhead is
the intentional cost of producing immutable, fenced migration contracts; it
does not sit on request routing or data operations.

## Security And Recovery Notes

Treat plan IDs, topology fingerprints, fencing tokens, and journal positions as
untrusted input at process boundaries. Validate them before persistence or
transport, authenticate the caller that can publish topology, and retain the
source snapshot until the target has been verified. A stale target is rejected
instead of silently completing a migration.
